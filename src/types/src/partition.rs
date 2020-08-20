use std::fmt;

#[derive(Debug)]
pub enum PartitionError {
    InvalidSyntax(String),
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSyntax(msg) => write!(f, "invalid partition syntax: {}", msg),
        }
    }
}

// returns a tuple (topic_name, idx)
pub fn decompose_partition_name(partition_name: &str) -> Result<(String, i32), PartitionError> {
    let dash_pos = partition_name.rfind('-');
    if dash_pos.is_none() {
        return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
    }

    let pos = dash_pos.unwrap();
    if (pos + 1) >= partition_name.len() {
        return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
    }

    let topic_name = &partition_name[..pos];
    let idx_string = &partition_name[(pos + 1)..];
    let idx = match idx_string.parse::<i32>() {
        Ok(n) => n,
        Err(_) => {
            return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
        }
    };

    Ok((topic_name.to_string(), idx))
}

pub fn create_partition_name(topic_name: &str, idx: &i32) -> String {
    format!("{}-{}", topic_name, idx)
}
