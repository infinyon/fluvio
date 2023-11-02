use std::str::FromStr;

use tide::Request;

use fluvio_hub_util::fvm::Channel;

#[async_std::main]
async fn main() -> tide::Result<()> {
    let mut app = tide::new();

    app.at("/hub/v1/fvm/pkgset/:package/:version/:arch")
        .get(get_pkgset);

    app.listen("127.0.0.1:9000").await?;

    Ok(())
}

async fn get_pkgset(mut req: Request<()>) -> tide::Result {
    let arch = req.param("arch")?;
    let package = req.param("package")?;
    let version = req.param("version")?;
    let channel = Channel::from_str(version)?;
    let mocked_pkgset = make_pkgset_mock(arch, package, channel);
    
    todo!()
}

fn make_pkgset_mock(arch: &str, package: &str, channel: Channel) {
    
}