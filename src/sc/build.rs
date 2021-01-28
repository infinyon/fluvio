fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::copy("../../VERSION", "./src/VERSION")?;
    Ok(())
}
