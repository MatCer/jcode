//! Agent defaults and swarm presentation configuration.

use serde::{Deserialize, Serialize};

/// Agent-specific model defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentsConfig {
    /// Opt in to explicit per-worker/per-node model selection. Default false.
    /// This does not change the configured pin or inheritance when no model is requested.
    pub swarm_model_selection: bool,
    /// Optional allowed selectors when selection is enabled. None adds no restriction;
    /// Some([]) denies new execution, including default/inherited routes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarm_allowed_models: Option<Vec<String>>,
    /// Optional default model override for spawned swarm/subagent sessions.
    ///
    /// Leave unset (or use `"inherit"` / `"coordinator"`) to have spawned swarm
    /// agents inherit the spawning coordinator's model. Set to a concrete model
    /// string to change the worker default. An explicit `model` in the swarm
    /// tool overrides this default only when `swarm_model_selection` is enabled.
    pub swarm_model: Option<String>,
    /// Optional default reasoning effort for spawned swarm/subagent sessions
    /// (`"low"`, `"medium"`, `"high"`, ...). Applied when a `swarm spawn`
    /// call does not pass an explicit `effort`. Leave unset to let workers
    /// inherit the provider-wide reasoning effort.
    pub swarm_effort: Option<String>,
    /// Default terminal mode for swarm-created agents.
    pub swarm_spawn_mode: SwarmSpawnMode,
    /// Maximum percentage (1-90) of the chat column height the inline swarm
    /// gallery band may occupy. Leave unset to use the built-in default (40%).
    /// Lower values keep more of the transcript visible; set near the minimum
    /// to effectively collapse the gallery to a thin strip.
    pub swarm_gallery_max_pct: Option<u8>,
    /// Layout of the inline swarm strip above the status line:
    /// `"vertical"` (default) lists one agent per row (session icon + status
    /// glyph + task), capped to a few lines; `"horizontal"` packs all agents
    /// as chips on a single row.
    #[serde(default)]
    pub swarm_strip_layout: SwarmStripLayout,
    /// Optional default model override for the memory sidecar.
    pub memory_model: Option<String>,
    /// Whether memory should use the sidecar for relevance/extraction.
    ///
    /// Defaults to `true`: the LLM precision-judge path is the only memory mode
    /// that is reliably productive (injection precision ~1.0), so memory uses it
    /// by default. Set to `false` only to deliberately opt into the lower-
    /// precision no-LLM hybrid path. When sidecar mode is on but no LLM backend
    /// is reachable, the memory runtime goes dormant instead of degrading to the
    /// no-LLM path.
    #[serde(default = "default_memory_sidecar_enabled")]
    pub memory_sidecar_enabled: bool,
    /// Minimum turns between Mode-2 memory reranks (cadence floor). The
    /// expensive listwise LLM rerank runs at most once per this many turns;
    /// skipped turns fall back to hybrid-ordered surfacing. A topic change or
    /// the first turn always forces a rerank regardless of cadence. 0 or 1 =
    /// rerank every turn (no gating). Default 3.
    #[serde(default = "default_memory_rerank_cadence")]
    pub memory_rerank_cadence: usize,
    /// Number of independent LLM rerank "judges" to run per fired rerank. Their
    /// votes are combined and only memories meeting `memory_rerank_min_agree`
    /// agreement are injected. 1 = single judge (cheapest). 2 = two judges must
    /// agree, which lifts injection precision to ~1.0 with ~100% clean-rate on
    /// no-memory turns (offline adjudication), at 2 LLM calls per fired turn.
    #[serde(default = "default_memory_rerank_votes")]
    pub memory_rerank_votes: usize,
    /// Minimum judge agreement (of `memory_rerank_votes`) required to inject a
    /// memory. Clamped to 1..=votes. Higher = stricter precision, lower recall.
    #[serde(default = "default_memory_rerank_min_agree")]
    pub memory_rerank_min_agree: usize,
    /// Which embedding backend memory dense-retrieval uses: `"local"` (bundled
    /// all-MiniLM-L6-v2 ONNX, default, no network) or `"openai"` (remote
    /// OpenAI/openai-compatible `/v1/embeddings`, opt-in, requires an
    /// `OPENAI_API_KEY`). A keyless `"openai"` setting silently degrades to
    /// local. Env override: `JCODE_MEMORY_EMBEDDING_BACKEND`.
    #[serde(default = "default_memory_embedding_backend")]
    pub memory_embedding_backend: String,
    /// OpenAI embedding model name when `memory_embedding_backend = "openai"`.
    /// Unset = `text-embedding-3-small`. Env: `JCODE_MEMORY_EMBEDDING_MODEL`.
    #[serde(default)]
    pub memory_embedding_model: Option<String>,
    /// Optional override for the embeddings API base URL (no trailing slash),
    /// for OpenAI-compatible gateways. Unset = `https://api.openai.com/v1`.
    /// Env: `JCODE_MEMORY_EMBEDDING_BASE_URL`.
    #[serde(default)]
    pub memory_embedding_base_url: Option<String>,
    /// Optional override for the remote embedding dimensionality (vector-space
    /// metadata / sanity checks). Unset = inferred from the model name.
    #[serde(default)]
    pub memory_embedding_dim: Option<usize>,
    /// Maximum number of live swarm worker agents in one swarm. This is the RAM
    /// safety budget for both recursive ad hoc spawning and deep-mode `run_plan`
    /// parallelism. Completed/stopped workers do not consume slots. Light mode
    /// still uses a smaller fixed fan-out. `0` disables this configurable guard,
    /// leaving only the absolute `MAX_SWARM_MEMBERS` hard cap.
    /// Env override: `JCODE_SWARM_MAX_CONCURRENT_AGENTS`.
    #[serde(default = "default_swarm_max_concurrent_agents")]
    pub swarm_max_concurrent_agents: usize,
}

fn default_swarm_max_concurrent_agents() -> usize {
    32
}

fn default_memory_embedding_backend() -> String {
    "local".to_string()
}

fn default_memory_sidecar_enabled() -> bool {
    true
}

fn default_memory_rerank_cadence() -> usize {
    3
}

fn default_memory_rerank_votes() -> usize {
    2
}

fn default_memory_rerank_min_agree() -> usize {
    2
}

impl Default for AgentsConfig {
    fn default() -> Self {
        Self {
            swarm_model_selection: false,
            swarm_allowed_models: None,
            swarm_model: None,
            swarm_effort: None,
            swarm_spawn_mode: SwarmSpawnMode::default(),
            swarm_gallery_max_pct: None,
            swarm_strip_layout: SwarmStripLayout::default(),
            memory_model: None,
            memory_sidecar_enabled: default_memory_sidecar_enabled(),
            memory_rerank_cadence: default_memory_rerank_cadence(),
            memory_rerank_votes: default_memory_rerank_votes(),
            memory_rerank_min_agree: default_memory_rerank_min_agree(),
            memory_embedding_backend: default_memory_embedding_backend(),
            memory_embedding_model: None,
            memory_embedding_base_url: None,
            memory_embedding_dim: None,
            swarm_max_concurrent_agents: default_swarm_max_concurrent_agents(),
        }
    }
}

/// How swarm-created agents should be spawned.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SwarmSpawnMode {
    /// Open a visible/headed terminal window. This was the historical default.
    Visible,
    /// Create the worker in-process without opening a terminal window.
    Headless,
    /// Like headless (no terminal window), but the coordinator renders a live
    /// inline gallery viewport of each worker's streaming output.
    #[default]
    Inline,
    /// Try visible first and fall back to headless if a window cannot be opened.
    Auto,
}

impl SwarmSpawnMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "visible" | "headed" => Some(Self::Visible),
            "headless" => Some(Self::Headless),
            "inline" => Some(Self::Inline),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }

    /// Canonical lowercase string for this mode (matches the config/env values).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::Headless => "headless",
            Self::Inline => "inline",
            Self::Auto => "auto",
        }
    }
}

/// Layout of the inline swarm strip shown above the status line.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SwarmStripLayout {
    /// One agent per row: session icon + status glyph + task label, capped to
    /// a few lines with a `+N more` overflow marker.
    #[default]
    Vertical,
    /// All agents packed as chips on a single row (the historical layout).
    Horizontal,
}

impl SwarmStripLayout {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "vertical" | "list" => Some(Self::Vertical),
            "horizontal" | "chips" | "strip" => Some(Self::Horizontal),
            _ => None,
        }
    }

    /// Canonical lowercase string for this layout (matches config/env values).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vertical => "vertical",
            Self::Horizontal => "horizontal",
        }
    }
}
