use anyhow::anyhow;

use crate::{
    config::ConnectorConfig,
    metadata::{ConnectorMetadata, Direction, Deployment, Secrets, Parameters},
};

pub fn validate_config(
    metadata: &ConnectorMetadata,
    config: &ConnectorConfig,
) -> anyhow::Result<()> {
    validate_direction(&metadata.direction, config)?;
    validate_deployment(&metadata.deployment, config)?;
    validate_secrets(&metadata.secrets, config)?;
    validate_parameters(&metadata.parameters, config)?;
    Ok(())
}

fn validate_direction(direction: &Direction, config: &ConnectorConfig) -> anyhow::Result<()> {
    let meta_is_source = direction.is_source();
    let cfg_is_source = config.is_source();
    if meta_is_source != cfg_is_source {
        let (meta_dir, cfg_dir) = if meta_is_source {
            ("source", "sink")
        } else {
            ("dest", "source")
        };
        return Err(anyhow!(
            "direction in metadata: '{}' does not correspond direction in config: '{}'",
            meta_dir,
            cfg_dir
        ));
    }
    Ok(())
}

fn validate_deployment(deployment: &Deployment, config: &ConnectorConfig) -> anyhow::Result<()> {
    let cfg_image = config.image();
    if !deployment.image.eq(&cfg_image) {
        return Err(anyhow!(
            "deployment image in metadata: '{}' mismatches image in config: '{}'",
            &deployment.image,
            cfg_image
        ));
    }
    Ok(())
}

fn validate_secrets(secrets: &Secrets, config: &ConnectorConfig) -> anyhow::Result<()> {
    for meta_secret in secrets.keys() {
        if !config.secrets.contains_key(meta_secret) {
            return Err(anyhow!(
                "missing required secret '{}' in config",
                meta_secret
            ));
        }
    }
    for cfg_secret in config.secrets.keys() {
        if !secrets.contains_key(cfg_secret) {
            return Err(anyhow!(
                "config secret '{}' is not defined in package metadata",
                cfg_secret
            ));
        }
    }
    Ok(())
}

fn validate_parameters(parameters: &Parameters, config: &ConnectorConfig) -> anyhow::Result<()> {
    for meta_param in parameters.iter() {
        if !config.parameters.contains_key(&meta_param.name) {
            return Err(anyhow!(
                "missing required parameter '{}' in config",
                &meta_param.name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use crate::{
        metadata::{Secret, SecretType, Parameter, ParameterType},
        config::{SecretString, ManagedConnectorParameterValue},
    };

    use super::*;

    #[test]
    fn test_validate_direction() {
        //given
        let source = Direction::source();
        let dest = Direction::dest();
        let source_config = ConnectorConfig {
            type_: "http-source".into(),
            ..Default::default()
        };
        let sink_config = ConnectorConfig {
            type_: "http-sink".into(),
            ..Default::default()
        };

        //when
        validate_direction(&source, &source_config).unwrap();
        validate_direction(&dest, &sink_config).unwrap();
        let res1 = validate_direction(&source, &sink_config);
        let res2 = validate_direction(&dest, &source_config);

        //then
        assert_eq!(
            res1.unwrap_err().to_string(),
            "direction in metadata: 'source' does not correspond direction in config: 'sink'"
        );
        assert_eq!(
            res2.unwrap_err().to_string(),
            "direction in metadata: 'dest' does not correspond direction in config: 'source'"
        );
    }

    #[test]
    fn test_validate_deployment() {
        //given
        let config = ConnectorConfig {
            type_: "http_source".into(),
            version: "latest".into(),
            ..Default::default()
        };
        let deployment1 = Deployment {
            image: "infinyon/fluvio-connect-http_source:latest".into(),
        };
        let deployment2 = Deployment {
            image: "infinyon/fluvio-connect-http_sink:latest".into(),
        };

        //when
        validate_deployment(&deployment1, &config).unwrap();
        let res = validate_deployment(&deployment2, &config);

        //then
        assert_eq!(res.unwrap_err().to_string(), "deployment image in metadata: 'infinyon/fluvio-connect-http_sink:latest' mismatches image in config: 'infinyon/fluvio-connect-http_source:latest'");
    }

    #[test]
    fn test_validate_secrets_missing() {
        //given
        let config = ConnectorConfig::default();
        let meta_secrets = Secrets::from(BTreeMap::from([(
            "secret_name".into(),
            Secret {
                ty: SecretType::Env,
                mount: None,
            },
        )]));

        //when
        let res = validate_secrets(&meta_secrets, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "missing required secret 'secret_name' in config"
        );
    }

    #[test]
    fn test_validate_secrets_undefined() {
        //given
        let config = ConnectorConfig {
            secrets: BTreeMap::from([("secret_name".into(), SecretString::from_str("").unwrap())]),
            ..Default::default()
        };

        //when
        let res = validate_secrets(&Secrets::default(), &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "config secret 'secret_name' is not defined in package metadata"
        );
    }

    #[test]
    fn test_validate_parameters_missing() {
        //given
        let config = ConnectorConfig::default();
        let meta_params = Parameters::from(vec![Parameter {
            name: "param_name".into(),
            description: None,
            ty: ParameterType::String,
        }]);

        //when
        let res = validate_parameters(&meta_params, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "missing required parameter 'param_name' in config"
        );
    }

    #[test]
    fn test_validate_config() {
        //given
        let config = ConnectorConfig {
            type_: "http-source".into(),
            version: "latest".into(),
            parameters: BTreeMap::from([(
                "param_name".into(),
                ManagedConnectorParameterValue::from("param_value"),
            )]),
            secrets: BTreeMap::from([("secret_name".into(), SecretString::from_str("").unwrap())]),
            ..Default::default()
        };
        let metadata = ConnectorMetadata {
            direction: Direction::source(),
            deployment: Deployment {
                image: "infinyon/fluvio-connect-http-source:latest".into(),
            },
            secrets: Secrets::from(BTreeMap::from([(
                "secret_name".into(),
                Secret {
                    ty: SecretType::Env,
                    mount: None,
                },
            )])),
            parameters: Parameters::from(vec![Parameter {
                name: "param_name".into(),
                description: None,
                ty: ParameterType::String,
            }]),
            ..Default::default()
        };

        //when
        validate_config(&metadata, &config).unwrap();

        //then
    }
}
