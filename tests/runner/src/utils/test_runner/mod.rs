// This mod is used to dynamically register tests via #[fluvio_test()] proc macro
// TODO: Maybe we want to rename that, and reserve meta for actual meta
// Suggestion: FluvioTestRegistry?
pub mod test_meta;
pub mod test_driver;
