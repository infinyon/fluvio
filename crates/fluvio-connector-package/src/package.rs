pub struct ConnectorMetadata {
    pub package: ConnectorPackage,
    pub direction: Direction,
    pub deployment: Deployment,
    pub parameters: Parameters,
}

pub struct ConnectorPackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub fluvio: FluvioSemVersion,
    pub api_version: FluvioSemVersion,
    pub description: Option<String>,
    pub license: Option<String>,
}

pub enum Direction {
    Source,
    Dest,
}

pub struct Deployment {
    image: String,
}

pub struct Parameters(Vec<Parameter>);

pub struct Parameter {
    pub name: String,
    pub description: Option<String>,
    pub ty: ParameterType,
}

pub enum ParamterType {
    String,
    Integer,
}
