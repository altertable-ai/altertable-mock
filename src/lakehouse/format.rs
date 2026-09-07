use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_schema::extension::{EXTENSION_TYPE_NAME_KEY, ExtensionType, Json};
use arrow_schema::{Field, Schema, SchemaRef};
use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use parquet::arrow::ArrowWriter;
use serde_json::{Map, Value};

pub(crate) const ALTERTABLE_ORIGINAL_TYPE_METADATA_KEY: &str = "altertable.original_type";
pub(crate) const ALTERTABLE_ORIGINAL_TYPE_JSON: &str = "JSON";
pub(crate) const ALTERTABLE_ORIGINAL_TYPE_VARIANT: &str = "VARIANT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Default,
    Csv,
    Jsonl,
    Parquet,
}

impl OutputFormat {
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.unwrap_or_default() {
            "" | "default" => Ok(Self::Default),
            "csv" => Ok(Self::Csv),
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            value => Err(format!("Unsupported query format: {value}")),
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Default | Self::Jsonl => "application/x-ndjson",
            Self::Csv => "text/csv",
            Self::Parquet => "application/parquet",
        }
    }

    pub fn is_default(self) -> bool {
        matches!(self, Self::Default)
    }
}

pub fn field_is_json_utf8(field: &Field) -> bool {
    if field.extension_type_name() == Some(Json::NAME) {
        return true;
    }
    field
        .metadata()
        .get(EXTENSION_TYPE_NAME_KEY)
        .is_some_and(|name| name == Json::NAME)
        || field
            .metadata()
            .get(ALTERTABLE_ORIGINAL_TYPE_METADATA_KEY)
            .is_some_and(|ty| {
                let ty = ty.to_ascii_uppercase();
                ty == ALTERTABLE_ORIGINAL_TYPE_JSON || ty == ALTERTABLE_ORIGINAL_TYPE_VARIANT
            })
}

/// Mirrors the real API's `arrow_type_to_string`, which resolves the DuckDB spelling of an Arrow
/// field for the `columns` line of a `POST /query` response.
pub fn arrow_type_to_string(field: &Field) -> anyhow::Result<String> {
    if field
        .metadata()
        .get(ALTERTABLE_ORIGINAL_TYPE_METADATA_KEY)
        .is_some_and(|ty| ty.eq_ignore_ascii_case(ALTERTABLE_ORIGINAL_TYPE_VARIANT))
    {
        return Ok(ALTERTABLE_ORIGINAL_TYPE_VARIANT.to_owned());
    }

    if field_is_json_utf8(field) {
        return Ok(ALTERTABLE_ORIGINAL_TYPE_JSON.to_owned());
    }

    let logical_type = duckdb::vtab::to_duckdb_logical_type(field.data_type())
        .map_err(|e| anyhow::anyhow!("failed to convert arrow type to duckdb logical type: {e}"))?;

    duckdb_type_to_string(&logical_type)
}

fn duckdb_type_to_string(logical_type: &LogicalTypeHandle) -> anyhow::Result<String> {
    let name = match logical_type.id() {
        LogicalTypeId::Boolean => "BOOLEAN".to_owned(),
        LogicalTypeId::Tinyint => "TINYINT".to_owned(),
        LogicalTypeId::Smallint => "SMALLINT".to_owned(),
        LogicalTypeId::Integer | LogicalTypeId::IntegerLiteral => "INTEGER".to_owned(),
        LogicalTypeId::Bigint => "BIGINT".to_owned(),
        LogicalTypeId::UTinyint => "UTINYINT".to_owned(),
        LogicalTypeId::USmallint => "USMALLINT".to_owned(),
        LogicalTypeId::UInteger => "UINTEGER".to_owned(),
        LogicalTypeId::UBigint => "UBIGINT".to_owned(),
        LogicalTypeId::Float => "FLOAT".to_owned(),
        LogicalTypeId::Double => "DOUBLE".to_owned(),
        LogicalTypeId::Timestamp => "TIMESTAMP".to_owned(),
        LogicalTypeId::Date => "DATE".to_owned(),
        LogicalTypeId::Time => "TIME".to_owned(),
        LogicalTypeId::Interval => "INTERVAL".to_owned(),
        LogicalTypeId::Hugeint => "HUGEINT".to_owned(),
        LogicalTypeId::Varchar | LogicalTypeId::StringLiteral => "VARCHAR".to_owned(),
        LogicalTypeId::Blob => "BLOB".to_owned(),
        LogicalTypeId::Decimal => {
            let precision = logical_type.decimal_width();
            let scale = logical_type.decimal_scale();
            format!("DECIMAL({precision}, {scale})")
        }
        LogicalTypeId::TimestampS => "TIMESTAMP_S".to_owned(),
        LogicalTypeId::TimestampMs => "TIMESTAMP_MS".to_owned(),
        LogicalTypeId::TimestampNs => "TIMESTAMP_NS".to_owned(),
        LogicalTypeId::Enum => "ENUM".to_owned(),
        LogicalTypeId::List => "LIST".to_owned(),
        LogicalTypeId::Struct => {
            let field_strs = (0..logical_type.num_children())
                .map(|i| {
                    let field_name = logical_type.child_name(i);
                    let field_type = duckdb_type_to_string(&logical_type.child(i))?;
                    Ok(format!("{field_name} {field_type}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            format!("STRUCT({})", field_strs.join(", "))
        }
        LogicalTypeId::Map => "MAP".to_owned(),
        LogicalTypeId::Uuid => "UUID".to_owned(),
        LogicalTypeId::Union => "UNION".to_owned(),
        LogicalTypeId::TimestampTZ => "TIMESTAMP WITH TIME ZONE".to_owned(),
        LogicalTypeId::Invalid => anyhow::bail!("invalid DuckDB type"),
        LogicalTypeId::Bit => "BIT".to_owned(),
        LogicalTypeId::TimeTZ => "TIMETZ".to_owned(),
        LogicalTypeId::UHugeint => "UHUGEINT".to_owned(),
        LogicalTypeId::Array => {
            let element = duckdb_type_to_string(&logical_type.child(0))?;
            format!("{element}[]")
        }
        LogicalTypeId::Any => "ANY".to_owned(),
        LogicalTypeId::Bignum => "DECIMAL".to_owned(),
        LogicalTypeId::SqlNull => "NULL".to_owned(),
        LogicalTypeId::TimeNs => "TIME_NS".to_owned(),
        _ => anyhow::bail!("unsupported DuckDB type (id={})", logical_type.raw_id()),
    };

    Ok(name)
}

pub fn record_batch_to_csv(batch: &RecordBatch, include_header: bool) -> anyhow::Result<Vec<u8>> {
    let mut writer = CsvWriterBuilder::new()
        .with_header(include_header)
        .build(Vec::new());
    writer
        .write(batch)
        .map_err(|e| anyhow::anyhow!("failed to serialize CSV batch: {e}"))?;
    Ok(writer.into_inner())
}

pub fn record_batch_to_jsonl(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let schema = batch.schema();
    let mut buf = Vec::new();

    for row in 0..batch.num_rows() {
        let mut object = Map::with_capacity(batch.num_columns());
        for (column_index, field) in schema.fields().iter().enumerate() {
            let value = array_value_to_json(batch.column(column_index).as_ref(), row, field)?;
            object.insert(field.name().clone(), value);
        }
        serde_json::to_writer(&mut buf, &object)
            .map_err(|e| anyhow::anyhow!("failed to serialize JSONL row: {e}"))?;
        buf.push(b'\n');
    }

    Ok(buf)
}

pub fn record_batches_to_parquet(
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> anyhow::Result<Vec<u8>> {
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)
        .map_err(|e| anyhow::anyhow!("failed to create Parquet writer: {e}"))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| anyhow::anyhow!("failed to serialize Parquet batch: {e}"))?;
    }
    writer
        .into_inner()
        .map_err(|e| anyhow::anyhow!("failed to finish Parquet export: {e}"))
}

pub fn record_batch_to_default_rows(batch: &RecordBatch) -> anyhow::Result<Vec<Vec<Value>>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(batch.num_columns());
        for (column_index, field) in schema.fields().iter().enumerate() {
            values.push(array_value_to_json(
                batch.column(column_index).as_ref(),
                row,
                field,
            )?);
        }
        rows.push(values);
    }

    Ok(rows)
}

fn array_value_to_json(
    array: &dyn arrow_array::Array,
    row: usize,
    field: &Field,
) -> anyhow::Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }

    if field_is_json_utf8(field) {
        let json = array.as_string::<i32>().value(row);
        return match serde_json::from_str(json) {
            Ok(value) => Ok(value),
            Err(_) => Ok(Value::String(json.to_owned())),
        };
    }

    let mut buf = Vec::new();
    let slice = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![array.slice(row, 1)],
    )
    .map_err(|e| anyhow::anyhow!("failed to slice record batch for JSON export: {e}"))?;
    let mut writer = arrow_json::WriterBuilder::new()
        .with_struct_mode(arrow_json::StructMode::ListOnly)
        .build::<_, arrow_json::writer::LineDelimited>(&mut buf);
    writer
        .write(&slice)
        .and_then(|_| writer.finish())
        .map_err(|e| anyhow::anyhow!("failed to encode column value as JSON: {e}"))?;
    let line = std::str::from_utf8(&buf)
        .map_err(|e| anyhow::anyhow!("JSON export output was not UTF-8: {e}"))?
        .trim();
    let row_values: Vec<Value> = serde_json::from_str(line)
        .map_err(|e| anyhow::anyhow!("failed to parse JSON export row: {e}"))?;
    row_values
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("JSON export row was empty"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::DataType;

    #[test]
    fn parses_supported_formats() {
        assert_eq!(OutputFormat::parse(None).unwrap(), OutputFormat::Default);
        assert_eq!(
            OutputFormat::parse(Some("")).unwrap(),
            OutputFormat::Default
        );
        assert_eq!(
            OutputFormat::parse(Some("default")).unwrap(),
            OutputFormat::Default
        );
        assert_eq!(OutputFormat::parse(Some("csv")).unwrap(), OutputFormat::Csv);
        assert_eq!(
            OutputFormat::parse(Some("jsonl")).unwrap(),
            OutputFormat::Jsonl
        );
        assert_eq!(
            OutputFormat::parse(Some("parquet")).unwrap(),
            OutputFormat::Parquet
        );
    }

    #[test]
    fn rejects_unsupported_formats() {
        let error = OutputFormat::parse(Some("json")).unwrap_err();
        assert!(error.contains("Unsupported query format"));
    }

    #[test]
    fn parses_json_utf8_cells_as_objects() {
        let field = Field::new("payload", DataType::Utf8, false)
            .with_metadata([(EXTENSION_TYPE_NAME_KEY.to_owned(), Json::NAME.to_owned())].into());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(StringArray::from(vec![r#"{"a":1}"#])) as _],
        )
        .unwrap();

        let rows = record_batch_to_default_rows(&batch).unwrap();
        assert_eq!(rows[0][0], serde_json::json!({"a": 1}));
    }
}
