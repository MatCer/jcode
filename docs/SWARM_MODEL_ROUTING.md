# Swarm model routing: operator activation

Routing is upstream default-off. Implementing or building this feature does not
activate it, replace a global prompt, install a binary, or restart a daemon.

## Before activation

1. Finish independent acceptance against the feature binary using an isolated
   home and socket. Confirm real initialized model/provider/API/effort for both
   OpenAI and Anthropic workers, heterogeneous DAG execution, deep-child policy,
   light-mode rejection, safe reuse, strict failures, and persisted retry/resume.
   Include visible startup, initialization failure cleanup, and no duplicate or
   orphan worker after presentation fallback. Unit tests do not replace this.
2. Verify the exact binary tested. A build does not change the shared daemon.
   A feature-local fallback build, when coordinated selfdev cannot prove it will
   avoid activation, is:
   ```bash
   CARGO_TARGET_DIR=$PWD/target cargo build --locked --jobs 6 --profile selfdev -p jcode --bin jcode
   ```
   Run from the feature worktree. Do not reload/install or change the
   shared-server symlink during implementation or acceptance.
3. Review project and global `swarm-prompt.md` overrides. Some older local prompts
   explicitly prohibit passing `model`, describe it as ignored, or recommend
   changing a global model pin between spawns. Those instructions contradict
   this feature. Only the operator should replace them, after acceptance and a
   deliberate activation decision. Do not edit global files automatically.

## Deliberate operator configuration

After acceptance, the operator may enable:

```toml
[agents]
swarm_model_selection = true
# Optional fallback for requests without model:
swarm_model = "gpt-6-astra"
# Optional restriction. Omit for no extra restriction, [] to deny new execution.
swarm_allowed_models = ["openai-oauth:gpt-6-astra", "claude-oauth:claude-opus-5"]
```

These routes are examples. Use `swarm list_models` on the intended runtime to
check availability and effective policy. Installation/restart/reload and config
activation are separate operator-owned actions. Do not swap global defaults
between concurrent spawns to simulate per-worker routing.

Suggested replacement for the obsolete routing paragraph, preserving the
operator's own task-to-model guidance separately:

> Check `swarm list_models` before choosing a route. If explicit selection is
> enabled, pass `model` and supported `effort` on workers or graph nodes according
> to the guidance below. Prefer the catalog's existing `prefix:model` selector
> when provider/authentication matters. If disabled, omit `model` and keep the
> configured default or coordinator inheritance. Do not change global config to
> work around rejection. Nested deep coordinators inherit the same guidance and
> may only narrow `allowed_models`. Treat conflicting node assignments and
> prohibited/unavailable routes as errors. Verify public actual execution and
> persisted identity before claiming the requested route or effort was used.

Normal/light workers still cannot spawn children. The user's semantic routing
table is prompt guidance, not a classifier or an authorization grant.

## Migration and recovery

Explicit `model` while disabled now errors, including `inherit`/`coordinator`.
Callers that relied on the old ignored-argument behavior must omit it until
activation. Blank strings are errors. Existing omitted fields deserialize as
before. Empty allowlists remain distinct from absent allowlists.

Disabling selection again prevents new explicit execution. Tightened policy can
block retries of pinned work but does not kill running workers or rewrite pins.
Do not delete or rewrite persistence files to bypass policy. Restore permission
deliberately or create a new task node through the supported graph workflow.
See [architecture](SWARM_ARCHITECTURE.md#opt-in-worker-routing) and
[node lifecycle](SWARM_TASK_GRAPH.md#opt-in-node-execution-constraints).

The SDK's existing GA wire surface does not mirror internal swarm commands.
This feature does not add an SDK swarm API or change unrelated GA shapes.
