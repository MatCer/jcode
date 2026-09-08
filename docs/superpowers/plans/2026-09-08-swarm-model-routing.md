# Opt-in per-worker and per-node model routing: implementation plan

> For agentic workers: implementation is explicitly authorized, including nested deep coordinators. Read /home/matcer/.agents/ENGINEERING-STANDARDS.md first. Work only in the feature worktree and only on assigned files.

**Goal:** Let root and nested deep-swarm orchestrators select worker models according to swarm-prompt.md, within operator-enabled, server-enforced constraints.
**Architecture:** Extend existing config, wire requests, spawn resolver and persisted DAG metadata. Keep model selection in the orchestrator and authorization in the server. Reuse provider-route resolution rather than introducing a second router.
**Tech stack:** Rust workspace, serde/TOML/JSON, current swarm runtime and provider implementations.
**Spec:** The behavioral contract below captures the user-approved direction. Configuration names are proposed, not currently supported on the verified base.
**Status:** Implementation authorized on 2026-09-08. Global config/prompt changes, deployment, shared-daemon reloads and edits in the focus-fix checkout remain prohibited. This revision is documentation-only, not evidence that routing policy is implemented.

## Branch safety

- Execution checkout: `/home/matcer/projects/tools/jcode-swarm-model-routing`, branch `feat/swarm-model-routing`.
- Verified base: `origin/master`, commit `4e85d4423cfd248236044564aa8e4d48ce0ae473`. Feature HEAD, `origin/master` and their merge-base all matched before this document commit. This verifies the local remote-tracking ref, not a new network fetch.
- Do not use local `master`: it carried the unrelated focus fix. Do not merge, copy or cherry-pick another worker's branch.
- Protected checkout: `/home/matcer/projects/tools/jcode`, branch `fix/focus-mode-1004-loop`. Never edit it, switch it, or run feature builds against its target directory.
- This document is now in the feature worktree. Source references below were rechecked against the verified upstream base using `agentgrep` and narrow reads. Do not initialize code indexes.
- Root caps this implementation at two workers overall. Nested feature behavior is authorized, but implementation workers do not recursively spawn without root authorization. Shared production files must be sequentially owned.

## Behavioral contract

### Operator control and precedence

Proposed config:

```toml
[agents]
swarm_model_selection = true
swarm_model = "gpt-6-astra"
# Optional exact model selectors or existing route-qualified selectors.
swarm_allowed_models = ["gpt-6-astra", "claude-fable-5-1", "claude-opus-5", "gpt-5.6-sol", "gpt-5.6-luna"]
```

- swarm_model_selection defaults false. With no explicit request, existing config-pin/inheritance behavior is unchanged.
- With the option enabled, explicit worker/node model wins over swarm_model; otherwise use swarm_model, then the requesting coordinator's effective identity.
- Explicit model requests when disabled return an actionable error before creating a worker or assigning a task. On the verified upstream base, explicit overrides already work without opt-in. Introducing default-off enforcement is therefore an intentional compatibility change from upstream, not merely removal of an ignored-argument warning. Preserve no-override config-pin/inheritance behavior and document how existing override users enable the policy.
- Missing allowlist imposes no additional model restriction beyond provider availability. An explicitly empty allowlist permits no new routed worker execution. Validate inherited/default routes too when the policy is enabled.
- Bare model entries allow that model through available providers. Users requiring provider restrictions must use existing route-qualified selectors. Normalize aliases before matching. Ambiguous explicit resolution must fail with route choices rather than arbitrarily selecting a provider.
- Do not assume a new route string syntax. Use the current resolver's supported syntax and test provider-qualified values from the real catalog.
- Keep requested effort separate from model selection. Preserve existing effort defaults. Report actual settings and reject unsupported explicit effort rather than claiming it was applied.

### Nested orchestration

- Normal/light mode retains its existing prohibition on workers spawning children.
- Deep-mode coordinators may select models for their children under the same operator policy.
- All descendants inherit effective restrictions. A parent may optionally provide a narrower allowed-model set for its subtree, but may not broaden its inherited set.
- Resolve authorization from trusted session ancestry, not caller-supplied role/root flags. Persist subtree restrictions so retries and resume cannot erase them.
- Revalidate current operator policy on new execution/retry. A newly tightened policy may block a persisted route; never silently replace it. Existing running workers are not killed by this feature.
- Choosing a child's model does not grant permission to change the caller's own assigned model or unrelated workers.
- This is server-side routing authorization, not a sandbox against an agent with unrestricted shell/config-edit access. Do not promise such a security boundary.

### DAG and lifecycle

- Add optional model and effort to wire node specs and persisted node execution metadata. Old graphs deserialize with no explicit choice.
- Node choice is authoritative for assignment. Reject conflicting assignment-time overrides rather than silently changing a persisted node constraint.
- Resolve and persist the concrete provider/model/API route at first execution. Retry, resume and replacement keep it unless the coordinator explicitly edits the pending/failed node through a supported operation.
- Never reuse an idle worker whose effective route or explicit effort requirement conflicts with the node. Prefer a matching worker, otherwise spawn if permitted, otherwise return an actionable blocked/error state.
- Apply identical validation to graph seed, expand, inject, direct spawn, assign_task, assign_next, fill_slots, run_plan, retry, reassign, replace and resume paths that exist on the execution base.
- Propagate the routing policy prompt to nested coordinators using existing prompt composition. Do not encode the user's model-role table in Rust.

### Observable guarantees

- Spawn/task status exposes requested model, resolved actual model, provider route and effort using existing event/status types.
- Unknown, prohibited or unavailable explicit routes fail before task claims/spawn side effects. Provider initialization failures must not become fallback successes.
- Idempotent spawn retries must not return an existing worker with a conflicting routing request.
- All three spawn presentation modes must enforce the same route. Validate visible startup identity as well as in-process inline/headless workers.
- The guarantee is that an explicit selection is obeyed or rejected. Prompt-driven semantic choice of the best model remains an LLM decision, not a deterministic classifier.

## Implementation sequence and ownership

### 1. Config, protocol and persisted execution contract

Relevant existing locations:
- crates/jcode-config-types/src/lib.rs: AgentsConfig and defaults.
- crates/jcode-base/src/config/default_file.rs: generated config documentation.
- crates/jcode-protocol/src/wire.rs: TaskGraphNodeSpec, spawn/assignment requests and response types.
- crates/jcode-protocol/src/protocol_tests/comm_requests.rs: active backward-compatible request round trips. The similarly named app-core file is not the primary protocol package test target.
- crates/jcode-plan/src/dag/mod.rs: NodeSpec and TaskNode; crates/jcode-plan/src/lib.rs: NodeMeta; crates/jcode-plan/src/bridge.rs: to_task_graph and apply_task_graph. Updating only TaskNode loses fields on the live plan round trip.

- [x] Recheck the feature base, exact data types and SDK surface using agentgrep. Do not initialize indexes.
- [ ] Add failing tests for config default false, absent versus empty allowlist, old request/node deserialization, and route/effort/policy round trips.
- [ ] Add minimal optional fields with serde defaults. Use one normalized route identity through all later stages.
- [ ] Define subtree policy persistence and requested-versus-resolved execution metadata without duplicating provider abstractions.
- [ ] Run focused tests, then commit only this task's files with a semantic title.

### 2. Enforced spawning and nested policy

Existing locations:
- crates/jcode-app-core/src/server/comm_session.rs: resolve_swarm_spawn_selection, spawn_swarm_agent.
- crates/jcode-app-core/src/server/comm_session_tests.rs.
- crates/jcode-app-core/src/server/swarm.rs: ancestry and depth helpers.
- crates/jcode-app-core/src/server/swarm_persistence.rs.
- crates/jcode-app-core/src/server/client_lifecycle.rs and client_lightweight_control.rs: request dispatch.

- [ ] Add failing precedence/disabled/allowlist/provider tests and a deep-child restriction test.
- [ ] Pass explicit route selection through dispatch to the existing resolver. Resolve authorization and availability before claims or worker creation.
- [ ] Preserve existing defaults, permit deep-child choices, enforce policy intersection, and prevent caller-supplied identity escalation.
- [ ] Cover inline/headless/visible preparation, initialization failure, duplicate request nonce with mismatched route, and config tightening after persistence.
- [ ] Run focused tests and commit.

### 3. DAG scheduling, reuse and durable retry

Existing locations:
- crates/jcode-app-core/src/server/comm_control.rs: handle_comm_assign_next and assignment selection.
- crates/jcode-app-core/src/server/comm_control_tests/dag_e2e.rs.
- crates/jcode-app-core/src/server/comm_control_tests/assign_next_dependency.rs and assign_next_metadata.rs.
- crates/jcode-app-core/src/server/comm_plan.rs and swarm_persistence.rs.
- crates/jcode-app-core/src/server/comm_graph.rs: spec_from_wire and seed/expand/complete/inject handlers; crates/jcode-plan/src/dag/ops.rs: seed equivalence, growth and generated gates.

- [ ] Add failing tests for two nodes with different models, a mismatched idle worker, matching-worker reuse and disabled spawn.
- [ ] Preserve model/effort through graph construction and mutations, filter candidates by actual route before existing affinity ranking, and resolve once at first execution.
- [ ] Add regression tests for retry/replacement/resume keeping the resolved route, revalidation under tightened policy, and conflicting assignment overrides.
- [ ] Verify failures release task claims and do not leave a plan indefinitely reported as progressing.
- [ ] Run focused tests and commit. Do not overlap this task's shared server files with task 2.

### 4. Public tool, SDK, prompt and observability

Existing locations:
- crates/jcode-app-core/src/tool/communicate.rs and communicate_tests.rs.
- crates/jcode-protocol/src/wire.rs response types.
- docs/SWARM_ARCHITECTURE.md and docs/SWARM_TASK_GRAPH.md.
- crates/jcode-base/src/prompt.rs and prompt_tests.rs: existing load_swarm_prompt composition and bundled-prompt assertions.
- SDK audit: sdk/typescript/src/protocol.ts exposes GA ApiRequest, not internal Request::Comm*; crates/jcode-sdk also uses the GA API. Do not create a new SDK swarm API just for this change. Update an existing mirror only if a changed GA shape actually requires it.

- [ ] Add failing schema/tool-output tests proving model is a supported field and nodes preserve it.
- [ ] Preserve already-supported model/effort parameters on direct spawn and assign_next. Add per-node routing to actual public graph inputs and show opt-in/effective restrictions in list_models. Avoid unrelated SDK API expansion.
- [ ] Expose actual initialized route and effort in spawn/node status, including actionable rejected-selection errors.
- [ ] Document opt-in, precedence, strict errors, nested policy and the limit of prompt-based semantic routing.
- [ ] Document activation guidance for contradictory user-local swarm prompt instructions, but do not edit global prompt/config files. Any later activation requires separate operator action.
- [ ] Run focused tests and commit.

### 5. Independent acceptance gate

- [ ] Use the root-authorized deep task graph with at most two workers overall and explicit dependencies: contracts -> spawn -> scheduler -> public surface -> independent acceptance. Route implementation and independent review to verified Astra execution. Shared files stay sequential. Do not confuse runtime deep-mode coverage with permission for workers to expand the implementation swarm.
- [ ] Cap cargo jobs at 6, check for existing heavy jobs first, and never run a second heavy suite concurrently.
- [ ] Build the feature-worktree TUI only. Use coordinated selfdev tooling only if it provably targets this checkout without activation or shared-daemon mutation; otherwise use the isolated local build command below. Never run build-reload or repoint launcher/shared-server symlinks. Use separate home/runtime/socket for acceptance.
- [ ] Exercise the public swarm tool with real available OpenAI and Anthropic routes: direct heterogeneous spawn, heterogeneous DAG, deep child spawn, and light-mode child rejection.
- [ ] Inspect actual persisted session/provider identity and public status, not a worker's self-reported model name.
- [ ] Exercise retry and daemon/session resume against persisted execution constraints, incompatible-worker reuse, forbidden model, unavailable provider and disabled opt-in.
- [ ] Re-run default no-override behavior and all focused suites. Check visible-mode startup via tester/frame if the implementation changes the UI path.
- [ ] On acceptance failure add a fix node and rerun the same acceptance gate. No successful-completion claim, PR or integration before the gate passes. Report external credential/provider blockers honestly.
- [ ] Only after acceptance offer deployment/reload and deliberate config/prompt activation. Do not silently modify the user's global runtime during feature development.

## Acceptance matrix

| Requirement | Required evidence |
| --- | --- |
| Preserve unrelated branch | Original branch/HEAD/status unchanged before and after planning and implementation |
| Opt-in and backward-compatible defaults | Old config/request/graph round trips and real disabled/default run |
| Per-worker selection | Two real spawned sessions with differing confirmed model/provider identities |
| Per-node selection | Two-node public task_graph/run_plan with the requested identities |
| Deep routing within inherited limits | Permitted child succeeds; widened child policy fails; light child spawn fails |
| No silent provider fallback | Invalid/unavailable route fails without worker/task claim |
| Durable selection | Retry, replacement and resumed plan retain confirmed resolved route |
| Safe reuse | Incompatible worker excluded; compatible worker reused |
| Prompt-driven orchestration | Root and nested prompts include current guidance; selected route is explicit in tool request |
| Accurate reporting | Requested and actual route/effort agree with persisted initialized session identity |

## Review notes

Alternatives rejected: changing the global pin between spawns is racy; restoring unrestricted overrides loses operator control; root-only routing unnecessarily blocks deep orchestration. No new router service, role classifier, or dependency is needed.

Plan self-review: requirements map to the acceptance matrix. The verified interfaces, narrow baseline results, scope risks and remaining acceptance access are recorded below. Baselines validate current upstream behavior only, not the unimplemented policy.

## Verified implementation map at 4e85d4423

Line references are for the verified base. Recheck after earlier implementation nodes change shared files.

| Concern | Exact current interface and required delta |
| --- | --- |
| Operator config | `crates/jcode-config-types/src/lib.rs:544` `AgentsConfig` already has `swarm_model: Option<String>` and `swarm_effort: Option<String>`; defaults start at `:654`. Add default-false selection and absent-versus-empty allowlist without changing existing defaults. Update `crates/jcode-base/src/config/default_file.rs` and tests in `crates/jcode-base/src/config_tests.rs`. |
| Direct wire | `crates/jcode-protocol/src/wire.rs:540` `Request::CommSpawn` and `:662` `CommAssignNext` already carry optional model/effort. `:14` `TaskGraphNodeSpec` has only id/content/kind/depends_on/priority. Extend graph inputs and any assignment constraint transport, not duplicate existing fields. |
| Resolver | `crates/jcode-app-core/src/server/comm_session.rs:366` `resolve_swarm_spawn_selection(Option<String>, Option<String>, &CoordinatorSpawnIdentity) -> SwarmSpawnSelection` already gives explicit requests precedence, including `inherit`/`coordinator`. `selection_for_concrete_model` is at `:333`. Add fallible policy/catalog admission around this behavior. Explicit blank selectors need a documented treatment, and inheritance sentinels must not bypass disabled opt-in or the allowlist. |
| Spawn execution | Same file `:574` `spawn_swarm_agent` accepts requested model and effort, `:847` `handle_comm_spawn` handles admission/replay, `:975` `handle_comm_list_models` reports the catalog. `client_lifecycle.rs:2513` and `client_lightweight_control.rs:411` are both dispatch paths. Internal scheduler calls bypass `handle_comm_spawn`, so validation only in the outer handler is insufficient. |
| Idempotency | `comm_session.rs:902` hashes model/effort along with request nonce into the mutation key. A conflicting model under the same nonce currently forms a different request, not a detected conflict. Add explicit mismatch coverage without weakening admission locking. `swarm_mutation_state.rs` persists replay responses. Replayed success after tightened policy must not hide a forbidden new execution. |
| Nested authorization | `comm_session.rs:1251` `ensure_spawn_coordinator_swarm` uses trusted `report_back_to_session_id` ancestry; `:1347` checks the root's persisted session effort for deep mode. Preserve root-based authorization and live-worker budget. `server/swarm.rs:41` `swarm_ancestors` and `:69` `swarm_spawn_depth` already handle ancestry. Do not use the requesting child's effort or self-declared role as authority. |
| Durable policy | `server/state.rs:188` `SwarmMember`, `durable_record` at `:243`, and `server/swarm_persistence.rs:133` `PersistedSwarmMember`/its flattened `crates/jcode-swarm-core/src/lib.rs:215` `SwarmMemberRecord` are the ancestry persistence path. Persist restriction narrowing through the existing durable record and hydration path, not only ephemeral `runtime`. |
| DAG durability | `jcode-plan/src/dag/mod.rs:349` `TaskNode`, `:407` `NodeSpec`, `jcode-plan/src/lib.rs:117` `NodeMeta`, `bridge.rs:94` `to_task_graph`, `:124` `apply_task_graph`. `server/swarm_persistence.rs:112` `PersistedVersionedPlan` already persists `node_meta`. All lift/lower/serde conversions must preserve requested and resolved constraints. |
| Graph mutations | `server/comm_graph.rs:25` `spec_from_wire` is the live conversion. `jcode-plan/src/dag/ops.rs:19` seed, `:78` seed equivalence and `:227` expand need routing-aware equality/preservation. Include generated critique/verify gates, synthesis re-wakes, injection, and `server/debug_swarm_write.rs:556` `DebugNodeSpec`/`:567` `debug_specs` in shape audits. |
| Assignment/reuse | `server/comm_control.rs:654` `resolve_assignment_target_for_task` claims a candidate; `:1849` `handle_comm_assign_next` receives model/effort but applies them only when spawning. Existing reused/explicit targets can therefore bypass routing today. Filter identity before claiming/ranking and validate all explicit target assignments. `:2044` `handle_comm_task_control` is the retry/reassign/replace lifecycle seam. |
| Real provider identity | `server/headless.rs:119` rebuilds the routed request via `MultiProvider::model_switch_request_for_session_route`; `:130` invokes `set_model` and `:136` checks model equivalence. This does not alone prove provider/API identity. `:149` currently only warns on effort failure. Strict explicit route and effort requests must fail rather than reuse these warning-only paths. Check `prepare_visible_spawn_session` in `comm_session.rs:428`, including presentation fallback at `:671`, without confusing terminal fallback with provider fallback. |
| Route reuse | `jcode-provider-core/src/lib.rs:777` `RouteSelection` is the structured runtime identity, `:787` builds it from `ModelRoute`, `:807` `routed_model_spec` owns route syntax. `jcode-base/src/provider/selection.rs:445` `model_switch_request_for_session_route` restores session identity. Supported examples are `openai-oauth:MODEL`, `openai-api:MODEL`, `claude-oauth:MODEL`, `claude-api:MODEL`; do not invent `MODEL@openai-oauth`. Match actual catalog routes and reject unavailable or ambiguous choices. |
| Public tool | `tool/communicate.rs:1961` `parameters_schema`, `:2061` model, `:2065` effort already exist. `spawn_assignment_session:1529`, assignment orchestration and `execute:2205` must preserve graph constraints. `format_swarm_model_list:1734` is existing status text. `tool/communicate_tests.rs:1200` already tests list-model rendering. |
| Reporting | `crates/jcode-protocol/src/lib.rs:491` `SwarmMemberRuntime` is existing ephemeral status. `wire.rs:1410` `CommSpawnResponse` and `CommListModelsResponse` (constructed in `comm_session.rs:995`) are response seams. Extend these only where needed, keeping durable execution truth in persisted session/node metadata rather than duplicating it in status. |
| Prompt | `crates/jcode-base/src/prompt.rs:77` `load_swarm_prompt` and existing prompt composition are the reuse point. `prompt_tests.rs:501` already asserts model/list_models guidance. No hard-coded private model-role table or user-global prompt edit. |

## KISS decisions and unresolved implementation hazards

1. Do not restore code that upstream already contains. The deliberate new behavior is operator opt-in, enforced allowlists/subtree narrowing, authoritative DAG route constraints, durable retries and accurate reporting.
2. Do not add a router service, model-role classifier, SDK subsystem, config hot-reload system or graph index. Resolve selectors against the existing provider catalog and use canonical route helpers. `jcode-plan` currently depends only on serde/serde_json; avoid pulling provider runtimes into the DAG crate. If a small serializable execution record is necessary, keep it data-only and adapt at the server boundary.
3. Persist requested versus resolved identity once, but preserve it through `NodeMeta` and graph operations. Do not create a second graph store. Missing optional fields must still deserialize old plans. Generated gates need an explicit inherited policy/default choice, not accidental loss of restrictions.
4. Graph validation, assignment validation and spawn initialization are distinct boundaries. Early admission prevents claims/worker side effects; initialized identity verification catches provider mis-selection. Both are needed. Do not replace one with the other or trust a worker's reported model name.
5. Subtree narrowing remains in the approved contract, but avoid a generic policy language. A normalized optional allowed-model list with intersection semantics is sufficient. Persist narrowing with server-owned ancestry. Missing ancestry must not silently become unrestricted during resume.
6. Existing retry/reassign/replace/resume paths must retain selection. There is no authorization to add an arbitrary node-edit API. If no supported pending/failed-node edit exists, reject conflicting changes and document how to submit a new node instead of building a new editor.
7. Preserve default effort behavior. Validate/reject unsupported *explicit* effort without turning unrelated inherited provider defaults into a new compatibility break. Do not treat model equivalence as proof that the requested API/auth route was used.
8. Reuse existing config refresh: `crates/jcode-base/src/config.rs:263` `config()` already uses a reloadable cache keyed by file metadata/environment, checked on a throttle. Capture current policy for each new admission and test tightening after that refresh as well as across isolated restart/resume. Do not add a new watcher or assume an old borrowed config reference updates in place. Existing running workers need not be killed.

## Executable validation and observed baseline

Always check `pgrep -a -f '(^|/)(cargo|rustc|rust-lld|ninja|make|vitest|pytest)( |$)'` and coordinate the sole heavy-test slot with the root before running builds/tests. Do not overlap heavy work even in different targets. Commands below run from the feature checkout and explicitly use its target directory, never the focus-fix target:

```bash
cd /home/matcer/projects/tools/jcode-swarm-model-routing
export CARGO_TARGET_DIR="$PWD/target"
rtk cargo test --locked --jobs 6 -p jcode-protocol --lib tests::test_comm_ -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-plan --lib dag::tests -- --test-threads=6
```

Observed on 2026-09-08, baseline `4e85d4423`: protocol **20 passed, 59 filtered out**; DAG **51 passed, 27 filtered out**. Both commands exited 0. No cargo/rustc/heavy test process was present before the run. The first protocol filter `protocol_tests::comm_requests` matched **zero** tests and is not evidence; it was corrected to the command above. The source file is included under `tests`, not a module named `protocol_tests::comm_requests`.

After implementation, run these sequential focused commands in the same directory/target environment. They are source-verified target/filter guidance, **not executed baseline results**:

```bash
rtk cargo test --locked --jobs 6 -p jcode-plan --lib bridge::tests -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-base --lib config::tests -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-app-core --lib comm_session_tests -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-app-core --lib assign_next -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-app-core --lib dag_e2e -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-app-core --lib swarm_persistence_tests -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-app-core --lib tool::communicate::tests -- --test-threads=6
rtk cargo test --locked --jobs 6 -p jcode-base --lib prompt::prompt_tests -- --test-threads=6
```

Check every filter runs nonzero tests. If test structure changes, use the same package with `-- --list` and a focused name filter. Do not infer success from a zero-test exit. Existing spawn resolver regressions include `resolve_swarm_spawn_model_requested_model_overrides_configured_pin`, `resolve_swarm_spawn_model_requested_inherit_overrides_configured_pin` and `resolve_swarm_spawn_model_auth_route_prefixes_pin_expected_routes`. Extend them with explicit policy fixtures rather than deleting valid upstream coverage.

## Required independent acceptance access

- A newly built feature TUI binary, isolated Unix socket, isolated `JCODE_HOME` and `JCODE_RUNTIME_DIR`, and a temporary opt-in config are required. `jcode-storage/src/lib.rs` owns home/runtime/durable-state resolution. A different socket alone does not isolate persisted sessions/config. Verify environment propagation to spawned visible clients and daemon children.
- Build fallback, only after reserving the sole heavy slot: `CARGO_TARGET_DIR="$PWD/target" rtk cargo build --locked --jobs 6 --profile selfdev -p jcode --bin jcode`. This is appropriate when coordinated selfdev cannot guarantee worktree-only output and no activation. Do not run installer or reload commands.
- Launch with the verified binary and explicit socket, for example `./target/selfdev/jcode run --no-update --socket "$JCODE_RUNTIME_DIR/routing.sock" '<acceptance prompt>'` with the isolated home/runtime exported. Confirm the actual server executable/version before measuring any result. Use runtime directories under the feature worktree or approved scratch, not the protected checkout.
- The acceptance runner needs authenticated, currently available OpenAI and Anthropic routes and permission for small real requests. Use existing auth access without printing/copying secrets into reports. Isolated-home credential provisioning must be agreed before execution; do not reset credentials or silently link writable global config/state. Missing cross-provider credentials is an acceptance blocker, not a reason to substitute mocks and claim success.
- Exercise public `swarm list_models`, direct `spawn`, `task_graph`/`run_plan`, a deep child and a light-mode rejection. Inspect initialized session model, provider key, API route, effort and persisted plan metadata. Exercise forbidden/unknown/unavailable selectors, incompatible reuse, same-nonce conflicting requests, empty allowlist, tightening, retry/replacement and restart/resume.
- Visible presentation needs a controlled terminal/tester attached to the isolated runtime, and proof its client resumed the prepared route. A unit test of visible preparation is useful but not visible-startup acceptance. Inline and headless share initialization yet each still needs public-path coverage.
- This document refinement did not build the TUI, access provider credentials, make model calls, restart any daemon, exercise visible startup, or perform runtime acceptance. Those are obligations of the independent acceptance node before any completion/deployment claim.
