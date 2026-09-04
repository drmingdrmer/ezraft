# EzRaft Development Guidelines

## Design Philosophy

EzRaft exists to give a simple API with extensibility for building Raft apps: the user implementing `EzStorage` (persistence) and `EzApp` (business logic) is still the product.

- `FileStorage` (`src/storage/file_storage.rs`) ships as a bundled `EzStorage` implementation so a first cluster runs without writing storage code, and it doubles as the worked example of the trait. It is a starting point, not the deployment answer: one JSON file per log entry and two `fsync`s per write. Its docs must keep saying so; never present it as production-ready. `EzStorage` stays the extension point.
- DX improvements simplify the contract the user implements (fewer operations, fewer inexplicable bounds, better docs) rather than adding more shipped components.
- Blanket impls derived from capabilities the user's own type declares (for example snapshots via `Serialize`) are API simplification and are welcome.
- `EzServer` (`src/server.rs`) has the same "sample, not the API" shape: its `/raft/*` and admin routes are production-shaped; `POST /api/write` and `POST /api/read` are a sample to copy and rewrite.
