use std::time::{SystemTime, UNIX_EPOCH};

use tide::Request;

#[async_std::main]
async fn main() -> tide::Result<()> {
    let mut app = tide::new();

    app.at("/get").get(resolve);

    app.listen("127.0.0.1:8080").await?;

    Ok(())
}

async fn resolve(mut req: Request<()>) -> tide::Result {
    Ok(format!("Hello").into())
}
