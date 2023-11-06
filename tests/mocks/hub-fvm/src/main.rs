mod fixtures;

use std::str::FromStr;

use tide::{Request, Response};

use fluvio_hub_util::fvm::Channel;

#[async_std::main]
async fn main() -> tide::Result<()> {
    let mut app = tide::new();

    app.at("/hub/v1/fvm/pkgset/:package/:version/:arch")
        .get(get_pkgset);

    app.listen("127.0.0.1:9000").await?;

    Ok(())
}

async fn get_pkgset(req: Request<()>) -> tide::Result {
    let version = req.param("version")?;
    let channel = Channel::from_str(version)?;
    let body = match channel {
        Channel::Latest => fixtures::DEFAULT_LATEST,
        Channel::Stable => fixtures::DEFAULT_STABLE,
        _ => fixtures::DEFAULT_0_10_14,
    };

    Ok(Response::builder(200)
        .body(body)
        .content_type("application/json")
        .build())
}
