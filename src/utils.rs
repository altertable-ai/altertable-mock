use std::{borrow::Cow, sync::Arc};

use duckdb::{Params, params_from_iter};

pub const MEMORY_DB: &str = "memory";
pub const TEMP_DB: &str = "temp";
pub const MAIN_SCHEMA: &str = "main";

pub fn empty_params() -> impl Params + Send + 'static {
    params_from_iter(Vec::<String>::new())
}

pub fn escape_identifier(s: &str) -> String {
    let mut result = String::with_capacity(s.len());

    for c in s.chars() {
        match c {
            '"' => result.push_str(r#""""#),
            _ => result.push(c),
        }
    }

    result
}

pub fn escape_literal(s: &str) -> String {
    let mut result = String::with_capacity(s.len());

    for c in s.chars() {
        match c {
            '\'' => result.push_str("''"),
            _ => result.push(c),
        }
    }

    result
}

pub trait SendableString: Send + 'static {
    fn as_str(&self) -> &str;
}

impl SendableString for String {
    fn as_str(&self) -> &str {
        self
    }
}

impl SendableString for &'static str {
    fn as_str(&self) -> &str {
        self
    }
}

impl SendableString for Cow<'static, str> {
    fn as_str(&self) -> &str {
        self
    }
}
impl SendableString for Arc<String> {
    fn as_str(&self) -> &str {
        self
    }
}
