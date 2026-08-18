#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum ComputeSize {
    XS,
    S,
    M,
    #[default]
    L,
    XL,
    XXL,
    XXXL,
    XXXXL,
}

impl std::str::FromStr for ComputeSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "XS" => Ok(Self::XS),
            "S" => Ok(Self::S),
            "M" => Ok(Self::M),
            "L" => Ok(Self::L),
            "XL" => Ok(Self::XL),
            "2XL" | "XXL" => Ok(Self::XXL),
            "3XL" | "XXXL" => Ok(Self::XXXL),
            "4XL" | "XXXXL" => Ok(Self::XXXXL),
            _ => Err(format!("invalid compute size: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ComputeSize;

    #[test]
    fn parses_known_sizes() {
        assert_eq!("XS".parse::<ComputeSize>().unwrap(), ComputeSize::XS);
        assert_eq!("2XL".parse::<ComputeSize>().unwrap(), ComputeSize::XXL);
    }

    #[test]
    fn rejects_auto_and_unknown() {
        for size in ["AUTO", "HUGE"] {
            let err = size.parse::<ComputeSize>().unwrap_err();
            assert!(
                err.contains("invalid compute size"),
                "expected invalid compute size for {size}, got {err}"
            );
        }
    }
}
