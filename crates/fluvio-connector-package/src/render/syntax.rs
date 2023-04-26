use minijinja::Syntax;

pub struct ConnectorTemplateSyntax(Syntax);

impl Default for ConnectorTemplateSyntax {
    fn default() -> Self {
        Self(Syntax {
            block_start: "${%".into(),
            block_end: "%}".into(),
            variable_start: "${{".into(),
            variable_end: "}}".into(),
            comment_start: "${#".into(),
            comment_end: "#}".into(),
        })
    }
}

impl From<ConnectorTemplateSyntax> for Syntax {
    fn from(syntax: ConnectorTemplateSyntax) -> Self {
        syntax.0
    }
}
