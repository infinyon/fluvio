# Release process

Bump up version `VERSION` file.

## Release crates

Find all crates that has been changed last release
```
cargo workspaces changed 
```

publish all crates

## Kick off release

Run Github action `release`