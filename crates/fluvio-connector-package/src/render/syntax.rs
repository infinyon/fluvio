use minijinja::syntax::SyntaxConfig;

pub struct ConnectorTemplateSyntax(SyntaxConfig);

impl Default for ConnectorTemplateSyntax {
    fn default() -> Self {
        let syntax = SyntaxConfig::builder()
            .block_delimiters("${%", "%}")
            .variable_delimiters("${{", "}}")
            .comment_delimiters("${#", "#}")
            .build()
            .expect("should always build syntax");
        Self(syntax)
    }
}

impl From<ConnectorTemplateSyntax> for SyntaxConfig {
    fn from(syntax: ConnectorTemplateSyntax) -> Self {
        syntax.0
    }
}

#[cfg(test)]
mod test {
    use super::ConnectorTemplateSyntax;

    #[test]
    fn test_syntax_default_does_not_panic() {
        let _ = ConnectorTemplateSyntax::default();
    }
}
