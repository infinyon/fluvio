# Release process

bump up version `VERSION` file

## Release crates

Find crates that has been changed last release
```
cargo ws changed 
```

publish all Crates

## Kick off release

Run Github action `release`