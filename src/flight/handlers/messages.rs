use std::collections::HashMap;

use arrow_flight::sql::ProstMessageExt;
use prost::Message;

// See https://arrow.apache.org/docs/format/Flight.html
// The Protobuf messages in this file are not yet available in the arrow-flight crate

#[derive(Clone, PartialEq, Message)]
pub struct SessionOptionValue {
    #[prost(oneof = "session_option_value::OptionValue", tags = "1, 2, 3, 4, 5")]
    pub option_value: Option<session_option_value::OptionValue>,
}

#[derive(Clone, PartialEq, Message)]
pub struct StringListValue {
    #[prost(string, repeated, tag = "1")]
    pub values: Vec<String>,
}

pub mod session_option_value {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum OptionValue {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(sfixed64, tag = "3")]
        Int64(i64),
        #[prost(double, tag = "4")]
        Double(f64),
        #[prost(message, tag = "5")]
        StringList(super::StringListValue),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionsRequest {
    #[prost(map = "string, message", tag = "1")]
    pub session_options: HashMap<String, SessionOptionValue>,
}

impl ProstMessageExt for SetSessionOptionsRequest {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.SetSessionOptionsRequest"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: Self::type_url().to_owned(),
            value: self.encode_to_vec().into(),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionsResult {
    #[prost(map = "string, message", tag = "1")]
    pub errors: HashMap<String, SetSessionOptionsResultError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionsResultError {
    #[prost(
        enumeration = "set_session_options_result_error::ErrorValue",
        tag = "1"
    )]
    pub value: i32,
}

pub mod set_session_options_result_error {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum ErrorValue {
        Unspecified = 0,
        InvalidName = 1,
        InvalidValue = 2,
        Error = 3,
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct CloseSessionResult {
    #[prost(enumeration = "close_session_result::Status", tag = "1")]
    pub status: i32,
}

pub mod close_session_result {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Unspecified = 0,
        Closed = 1,
        Closing = 2,
        NotCloseable = 3,
    }
}
