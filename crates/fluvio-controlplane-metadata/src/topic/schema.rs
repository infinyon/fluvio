use anyhow::{anyhow, Result};


use fluvio_protocol::{Encoder, Decoder};



#[derive(Debug, Clone, PartialEq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]

pub struct TopicSchema {

}



pub enum Schema {

}


#[derive(Debug, Clone, PartialEq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ColumnSchema {

    #[fluvio(min_version = 11)]
    #[cfg_attr(feature = "use_serde", serde(default))]
    columns: Vec<ColumnDef>,
}


impl ColumnSchema {

    pub fn get_columns(&self) -> &Vec<ColumnDef> {
        &self.columns
    }

    pub fn set_columns(&mut self, columns: Vec<ColumnDef>) {
        self.columns = columns;
    }
}




#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]

pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
}

impl ColumnDef {
    /// create new mapping from name and type encoded in a string
    /// format is `name:ty`, if there is no ty, then it is assumed to be string
    /// ty is one of i = Integer, l = Long, f = Float, d = Double, s = String, t = TimestampMs
    /// if ty is not recognized, then this will return error
    pub fn from_dsl(name_ty: &str) -> Result<Self> {
        // check if name contains type
        let mut parts = name_ty.split(':');
        let name = parts.next().unwrap().to_string(); // always name
        let ty = if let Some(ty_string) = parts.next() {
            match ty_string {
                "i" => ColumnType::Integer,
                "l" => ColumnType::Long,
                "f" => ColumnType::Float,
                "d" => ColumnType::Double,
                "s" => ColumnType::String,
                "t" => ColumnType::Timestamp,
                _ => return Err(anyhow!("Unknown type: {}", ty_string)),
            }
        } else {
            ColumnType::String
        };

        Ok(Self { name, ty })
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "use_serde", serde(rename_all = "lowercase"))]
pub enum ColumnType {
    #[fluvio(tag = 0)]
    #[default]
    Bool,
    #[fluvio(tag = 1)]
    String,
    #[fluvio(tag = 2)]
    Integer,
    #[fluvio(tag = 3)]
    Long,
    #[fluvio(tag = 4)]
    Float,
    #[fluvio(tag = 5)]
    Double,
    #[fluvio(tag = 6)]
    Timestamp,
}



#[cfg(test)]
pub mod test {

    use super::*;

    #[test]
    fn test_column_def() {
        let colum1 = ColumnDef::from_dsl("colum1:i").expect("colum1");
        assert_eq!(colum1.name, "colum1");

        let colum2 = ColumnDef::from_dsl("colum2:f").expect("colum2");
        assert_eq!(colum2.name, "colum2");
        assert_eq!(colum2.ty, ColumnType::Float);

        let colum3 = ColumnDef::from_dsl("colum3:l").expect("colum1");
        assert_eq!(colum3.name, "colum3");
        assert_eq!(colum3.ty, ColumnType::Long);

        let colum4 = ColumnDef::from_dsl("colum4:s").expect("colum1");
        assert_eq!(colum4.name, "colum4");
        assert_eq!(colum4.ty, ColumnType::String);

        let colum5 = ColumnDef::from_dsl("colum5:t").expect("colum1");
        assert_eq!(colum5.name, "colum5");
        assert_eq!(colum5.ty, ColumnType::Timestamp);

        let colum6 = ColumnDef::from_dsl("colum6").expect("colum1");
        assert_eq!(colum6.name, "colum6");
        assert_eq!(colum6.ty, ColumnType::String);

        assert!(ColumnDef::from_dsl("colum7:z").is_err());
    }
}