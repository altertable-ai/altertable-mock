use std::{ffi::CString, ptr, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use duckdb::ffi::{
    DUCKDB_TYPE, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
    DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_DUCKDB_TYPE_DATE, DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    DUCKDB_TYPE_DUCKDB_TYPE_INVALID, DUCKDB_TYPE_DUCKDB_TYPE_LIST,
    DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_STRUCT, DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess, duckdb_decimal_scale, duckdb_decimal_width,
    duckdb_destroy_logical_type, duckdb_destroy_prepare, duckdb_get_type_id,
    duckdb_list_type_child_type, duckdb_logical_type, duckdb_nparams, duckdb_param_logical_type,
    duckdb_param_type, duckdb_parameter_name, duckdb_prepare, duckdb_prepare_error,
    duckdb_prepared_statement, duckdb_prepared_statement_column_count,
    duckdb_prepared_statement_column_logical_type, duckdb_prepared_statement_column_name,
    duckdb_struct_type_child_count, duckdb_struct_type_child_name, duckdb_struct_type_child_type,
};

struct PreparedStatementGuard {
    stmt: *mut duckdb_prepared_statement,
}

impl Drop for PreparedStatementGuard {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_prepare(self.stmt);
        }
    }
}

// Since `duckdb::Statement` does not provide a way to get the schema without executing the statement,
// we need to use the low-level FFI API
pub fn extract_schema(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<Schema> {
    let c_sql = CString::new(sql).map_err(|e| anyhow::anyhow!("Invalid SQL: {e}"))?;

    unsafe {
        let raw_conn = conn.raw_connection();

        let mut prep: duckdb_prepared_statement = ptr::null_mut();
        let _guard = PreparedStatementGuard { stmt: &mut prep };

        let result = duckdb_prepare(raw_conn, c_sql.as_ptr(), &mut prep);
        if result != DuckDBSuccess {
            let error = duckdb_prepare_error(prep);
            let error_str = std::ffi::CStr::from_ptr(error)
                .to_string_lossy()
                .into_owned();
            return Err(anyhow::anyhow!("Failed to prepare statement: {error_str}"));
        }

        let ncols = duckdb_prepared_statement_column_count(prep);
        if ncols == 0 {
            return Err(anyhow::anyhow!(
                "Statement has no columns to extract schema from"
            ));
        }

        let mut fields = Vec::with_capacity(ncols as usize);

        for i in 0..ncols {
            let c_name = duckdb_prepared_statement_column_name(prep, i);
            let name = std::ffi::CStr::from_ptr(c_name)
                .to_string_lossy()
                .into_owned();

            let ltyp = duckdb_prepared_statement_column_logical_type(prep, i);
            let dt = logical_type_to_arrow_type(ltyp);
            duckdb_destroy_logical_type(&mut (ltyp as duckdb_logical_type));

            fields.push(Field::new(name, dt, true));
        }

        Ok(Schema::new(fields))
    }
}

pub fn extract_parameter_schema(
    conn: &duckdb::Connection,
    sql: &str,
) -> anyhow::Result<Option<Schema>> {
    let c_sql = CString::new(sql).map_err(|e| anyhow::anyhow!("Invalid SQL: {e}"))?;

    unsafe {
        let raw_conn = conn.raw_connection();

        let mut prep: duckdb_prepared_statement = ptr::null_mut();
        let _guard = PreparedStatementGuard { stmt: &mut prep };

        let result = duckdb_prepare(raw_conn, c_sql.as_ptr(), &mut prep);
        if result != DuckDBSuccess {
            let error = duckdb_prepare_error(prep);
            let error_str = std::ffi::CStr::from_ptr(error)
                .to_string_lossy()
                .into_owned();
            return Err(anyhow::anyhow!("Failed to prepare statement: {error_str}"));
        }

        let nparams = duckdb_nparams(prep);
        if nparams == 0 {
            return Ok(None);
        }

        let mut fields = Vec::with_capacity(nparams as usize);

        for i in 0..nparams {
            let c_name = duckdb_parameter_name(prep, i);
            let name = if c_name.is_null() {
                format!("parameter_{}", i + 1)
            } else {
                let name_str = std::ffi::CStr::from_ptr(c_name)
                    .to_string_lossy()
                    .into_owned();
                duckdb::ffi::duckdb_free(c_name as *mut std::ffi::c_void);
                if name_str.is_empty() || name_str.parse::<usize>().is_ok() {
                    format!("parameter_{}", i + 1)
                } else {
                    name_str
                }
            };

            let ltyp = duckdb_param_logical_type(prep, i);
            let dt = if ltyp.is_null() {
                let simple_type = duckdb_param_type(prep, i);
                if simple_type == DUCKDB_TYPE_DUCKDB_TYPE_INVALID {
                    DataType::Utf8
                } else {
                    simple_type_to_arrow_type(simple_type)
                }
            } else {
                let arrow_type = logical_type_to_arrow_type(ltyp);
                duckdb_destroy_logical_type(&mut (ltyp as duckdb_logical_type));
                arrow_type
            };

            fields.push(Field::new(name, dt, true));
        }

        Ok(Some(Schema::new(fields)))
    }
}

unsafe fn simple_type_to_arrow_type(type_id: DUCKDB_TYPE) -> DataType {
    match type_id {
        DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => DataType::Boolean,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => DataType::Int8,
        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => DataType::Int16,
        DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => DataType::Int32,
        DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => DataType::Int64,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => DataType::UInt8,
        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => DataType::UInt16,
        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => DataType::UInt32,
        DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => DataType::UInt64,
        DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => DataType::Float32,
        DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => DataType::Float64,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => DataType::Utf8,
        DUCKDB_TYPE_DUCKDB_TYPE_BLOB => DataType::Binary,
        DUCKDB_TYPE_DUCKDB_TYPE_DATE => DataType::Date32,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME => DataType::Time64(TimeUnit::Microsecond),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => DataType::Timestamp(TimeUnit::Millisecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => DataType::Timestamp(TimeUnit::Nanosecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => DataType::Timestamp(TimeUnit::Second, None),
        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => DataType::Duration(TimeUnit::Microsecond),
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => DataType::Decimal128(38, 0),
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => DataType::Decimal128(38, 10),
        DUCKDB_TYPE_DUCKDB_TYPE_UUID => DataType::FixedSizeBinary(16),
        _ => {
            tracing::warn!("Unknown DuckDB simple type ID: {type_id}, using Utf8 as fallback");
            DataType::Utf8
        }
    }
}

unsafe fn logical_type_to_arrow_type(ltyp: duckdb_logical_type) -> DataType {
    let type_id: DUCKDB_TYPE = unsafe { duckdb_get_type_id(ltyp) };

    match type_id {
        DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => DataType::Boolean,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => DataType::Int8,
        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => DataType::Int16,
        DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => DataType::Int32,
        DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => DataType::Int64,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => DataType::UInt8,
        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => DataType::UInt16,
        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => DataType::UInt32,
        DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => DataType::UInt64,
        DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => DataType::Float32,
        DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => DataType::Float64,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => DataType::Utf8,
        DUCKDB_TYPE_DUCKDB_TYPE_BLOB => DataType::Binary,
        DUCKDB_TYPE_DUCKDB_TYPE_DATE => DataType::Date32,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME => DataType::Time64(TimeUnit::Microsecond),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => DataType::Timestamp(TimeUnit::Millisecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => DataType::Timestamp(TimeUnit::Nanosecond, None),
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => DataType::Timestamp(TimeUnit::Second, None),
        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => DataType::Duration(TimeUnit::Microsecond),
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => DataType::Decimal128(38, 0),
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
            let precision = unsafe { duckdb_decimal_width(ltyp) };
            let scale = unsafe { duckdb_decimal_scale(ltyp) };
            DataType::Decimal128(precision, scale as i8)
        }
        DUCKDB_TYPE_DUCKDB_TYPE_UUID => DataType::FixedSizeBinary(16),
        DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
            let child_type = unsafe { duckdb_list_type_child_type(ltyp) };
            let child_arrow_type = unsafe { logical_type_to_arrow_type(child_type) };
            DataType::List(Arc::new(Field::new("item", child_arrow_type, true)))
        }
        DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
            let member_count = unsafe { duckdb_struct_type_child_count(ltyp) };
            let mut struct_fields = Vec::with_capacity(member_count as usize);

            for i in 0..member_count {
                let member_name_ptr = unsafe { duckdb_struct_type_child_name(ltyp, i) };
                let member_name = unsafe { std::ffi::CStr::from_ptr(member_name_ptr) }
                    .to_string_lossy()
                    .into_owned();

                let member_type = unsafe { duckdb_struct_type_child_type(ltyp, i) };
                let member_arrow_type = unsafe { logical_type_to_arrow_type(member_type) };

                struct_fields.push(Field::new(member_name, member_arrow_type, true));
            }

            DataType::Struct(struct_fields.into())
        }
        _ => {
            tracing::warn!("Unknown DuckDB type ID: {type_id}, using Null as fallback");
            DataType::Null
        }
    }
}
