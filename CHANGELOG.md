# Changelog

## v0.1.1

- Pin `openraft` to `=0.10.0-alpha.32`. Its pre-release series breaks API
  between alphas, and ezraft implements its storage, network and type-config
  traits, so a caret requirement let a downstream `cargo update` pull an
  incompatible alpha into a build of ezraft that had not changed.

## v0.1.0

Initial release. Imported from openraft's `experimental/ezraft`, where the
crate was developed as a workspace member.
