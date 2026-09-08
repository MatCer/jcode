//! Durable node metadata and server-selected execution identity.

use serde::{Deserialize, Serialize};

/// Concrete execution identity selected by the server at first dispatch.
///
/// Data-only adapter for provider-core's `RouteSelection`, not another resolver.
/// `provider_key` is `RouteSelection.runtime_key.stable_id()` (including compatible
/// profile ids), `api_method` and `provider_label` are copied verbatim from that
/// selection. The server must reconstruct/validate the route against its catalog,
/// never infer provider identity from model equivalence alone. `effort` records
/// the concrete execution setting, not the original effort request. None means
/// no known setting. This pinned target alone is not evidence of initialization:
/// only report actual execution after checking the initialized session identity.
/// Persist this through retries/replacement/resume and revalidate current policy
/// before new execution. Untrusted node specs must not supply this record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedExecution {
    pub model: String,
    pub provider_key: String,
    pub api_method: String,
    pub provider_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// Per-node task-DAG metadata, stored as a side map on `VersionedPlan` keyed by
/// plan item id. This mirrors the `task_progress` side-map pattern so existing
/// `PlanItem` construction sites stay unchanged while the DAG engine gains the
/// extra structure it needs (composite/gate mechanics + typed artifacts).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMeta {
    /// Original explicit selector. None keeps configured/inherited selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Original explicit effort, separate from the effective execution setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_execution: Option<ResolvedExecution>,
    /// Trusted server snapshot of inherited node restrictions. Never accepted from wire specs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_models: Option<Vec<String>>,
    /// Terminal-action kind: "explore" | "implement" | "verify" | "fix" |
    /// "synthesize" | "critique". Defaults to a plain task when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The composite node this was decomposed from, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// True once decomposed into children (composite join/synthesis point).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub expanded: bool,
    /// True if this node is an auto-inserted critique/verify gate.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_gate: bool,
    /// The agent that planned this node's decomposition (composite owner). Kept
    /// separately from `PlanItem.assigned_to` so a re-queued composite can be
    /// auto-scheduled (assigned_to cleared) while still preferring its original
    /// planner for the synthesis step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner: Option<String>,
    /// The typed handoff artifact, present once the node completes. Serialized as
    /// JSON text so the protocol/persistence layers need no extra types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_json: Option<String>,
    /// Where the node came from: "seed" | "expand" | "gap" | "gate". Powers the
    /// growth accounting (seeded vs machinery-grown) on status surfaces. Absent
    /// on legacy plans, which count as seeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}
