//! The application a cluster replicates

use std::fmt;

use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The application: request/response types, state, and one method of business logic
///
/// The implementing type IS the application state - a struct holding your data. The framework
/// derives snapshots from it via serde: a snapshot is the serialized state, installing one
/// replaces the state with the deserialized bytes. That makes whole-state serialization the
/// scope of this crate; it serves the coordination/metadata class of app whose state fits in
/// memory (ZooKeeper snapshots the same way). An app whose snapshot is a streamed checkpoint
/// of something larger builds on openraft directly.
///
/// # Example (KV store)
///
/// ```
/// use std::collections::BTreeMap;
///
/// use async_trait::async_trait;
/// use ezraft::EzApp;
/// use serde::Deserialize;
/// use serde::Serialize;
///
/// #[derive(Serialize, Deserialize, Debug, Clone)]
/// enum Request {
///     Set { key: String, value: String },
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct Response {
///     value: Option<String>,
/// }
///
/// #[derive(Default, Serialize, Deserialize)]
/// struct KvApp {
///     data: BTreeMap<String, String>,
/// }
///
/// #[async_trait]
/// impl EzApp for KvApp {
///     type Request = Request;
///     type Response = Response;
///
///     async fn apply(&mut self, req: Request) -> Response {
///         match req {
///             // The replaced value, if any: the caller learns what was there
///             // without a second round trip.
///             Request::Set { key, value } => Response {
///                 value: self.data.insert(key, value),
///             },
///         }
///     }
///
///     fn read(&self, key: &str) -> Option<serde_json::Value> {
///         self.data.get(key).map(|v| serde_json::Value::String(v.clone()))
///     }
/// }
/// ```
#[async_trait]
pub trait EzApp: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Application request type
    ///
    /// Serde carries it over the wire, `Clone` keeps a copy for forwarding to the leader, and
    /// `Debug` names it in logs and errors. All three derive. openraft wants `Display` here too;
    /// [`EzRequest`](crate::EzRequest) answers that with `Debug`, so it stays off this trait.
    type Request: fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static;

    /// Application response type
    ///
    /// Produced by [`apply`](Self::apply) and carried back over the wire to whichever node
    /// forwarded the write, hence the serde bounds.
    type Response: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Apply a committed request to the state machine
    ///
    /// This is where your business logic goes. A request arrives here only once it is
    /// committed - stored by a majority of the nodes, and past the point of being lost - and
    /// every node applies the same requests in the same order, which is what keeps their state
    /// identical. The method is called sequentially, in log order, exactly once per committed
    /// entry.
    async fn apply(&mut self, req: Self::Request) -> Self::Response;

    /// Answer a keyed read against the local state
    ///
    /// Powers `GET /api/read?key=...`: the write API puts keys in, this reads one back. What a
    /// "key" means is the app's to define; return `None` for a key the app does not hold (a
    /// 404 over HTTP). Answer from your own data structures - an indexed lookup, not a scan of
    /// the serialized state.
    ///
    /// An app with nothing to expose this way returns `None` for every key; there is no default,
    /// so that declining is a decision the app states rather than one it inherits.
    fn read(&self, key: &str) -> Option<serde_json::Value>;
}
