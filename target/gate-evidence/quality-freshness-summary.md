# Public routing freshness fixture regression

## Scope and result

- Source commit: `dbd988c9bba2fd1eb2b0f901134604628297db8c` (`test: isolate public swarm routing from embedding downloads`). Checkout `/home/matcer/projects/tools/jcode-swarm-model-routing`, branch `feat/swarm-model-routing` only.
- Starting source: `d4973bebe10739a7d7c7526b171c9c185d1bb3f9`, after all four required cleanup commits. Only changed source is `crates/jcode-app-core/src/tool/communicate_tests/routing.rs`. The evidence-summary commit changes no source.
- Exact tested source SHA256: `d210de8bf5aaa87114a4f51c1b5ba79029d2754affdac25a9e3d84912a9cd1c3`. All GREEN and suite runs recorded this hash before execution. Committed source matches. Full clean-source/log provenance: `quality-freshness-provenance.json`.
- **Fixture isolation fixed, not a production sampler fix.** Three fresh-process isolated passes expose completed, idle, freshly verified actual execution. All eight requested/related regression families pass. No aggregate pass substitutes for the three isolated passes.
- Sole heavy slot reserved with lion at 20:37 UTC, no compiler present before first run, sequential throughout, released at 20:44:35 UTC. No children, other branches, global configuration, live LLM providers, daemon, install, reload, or baseline-source/budget changes.

## Exact test after extraction

`tool::communicate::tests::routing_public_tool_exact_node_spawn_status_policy_and_driver_failures`

The name is unchanged because `communicate_tests.rs:1522` uses `include!`, not a nested module. Body now starts at `communicate_tests/routing.rs:223`. Test remains the existing `#[tokio::test]` current-thread runtime. The original three-second freshness deadline, five-second driver deadlines, provider's 100ms stream delay, and every original routing/policy/initialized identity/effort assertion remain. No sleeps, retries, cached-actual substitution, or timeout inflation added.

## Root cause and RED evidence

1. Original archived `schema-cap-freshness-{1,2,3}.log` each failed with running/busy and actual unknown. Read these and `schema-cap-summary.md`, the child contract, standards, and all four cleanup summaries before diagnosis.
2. New unchanged-source RED: `quality-freshness-red.log`, exit101, 0 passed/1 failed, 9.62s. Freshness timeout reproduced after extraction. This run's last-status string was empty, consistent with the status request failing to progress before the timer expired.
3. Two bounded boundary-instrumented REDs: `quality-freshness-diagnostic.log`, exit101, 9.57s; `quality-freshness-diagnostic-2.log`, exit101, 9.72s. Both preserve running/busy, correct pinned high identity, actual unknown. Startup tool definitions/system prompt/messages/prompt accounting finish in milliseconds. `Provider::complete` is reached, but the double's 100ms stream timer never emits text or MessageEnd before the freshness deadline. No post-stream finalization boundary is reached.
4. After those three RED attempts, lion explicitly approved one distinct bounded GDB stack capture, not another blind repetition. `quality-freshness-stack.log:323-361`: current-thread executor is blocked in `Server::spawn_background_tasks -> capture_runtime_memory_process_sample -> capture_runtime_memory_common_sample -> jcode_base::embedding::stats -> Mutex::lock_contended`.
5. Lock owner: `quality-freshness-stack.log:279-298` shows `memory_agent -> embedding_backend::embed_query_active -> embedding::embed -> get_embedder -> Embedder::load -> download_model`. The fresh JCODE_HOME fixture omitted `features.memory=false`, so a routing provider-double test unintentionally started real embedding-model retrieval. A concurrent server memory sample synchronously waited on that cache mutex, starving provider/status timers. `server.rs:424` and read-only `jcode-base/src/embedding.rs:306` confirm the sampler's blocking call.
6. GDB runner/cargo wrapper exits0, but the inferior test explicitly fails and exits octal0145 (=101). This is diagnostic failure evidence, **not GREEN**. Stack command and monotonic boundary timestamps are retained in `quality-freshness-stack.gdb` and `.log`. GDB attached only its own spawned isolated test process. The stack exposed an attempted embedding model download, not a live LLM provider call. Its download success/failure was not established.

Earlier aggregate success is consistent with process-global embedding state already having been initialized by other tests, but no warm-cache A/B experiment was performed. The exact isolated blocker is stack-proven; aggregate order is not used as proof or a fix.

## Minimal approved change

- `communicate_tests/routing.rs:232-237`: write `[features]\nmemory = false` only into this fixture's existing temporary config, invalidate the cache as before, and assert the loaded feature is disabled. Reuses the established isolation pattern in `server/comm_session_effort_tests.rs:105`. It does not prewarm a cache or change production defaults.
- `communicate_tests/routing.rs:330-336`: preserve the exact verified actual model/provider/API/high-effort predicate and additionally require `Activity: idle`. Print the passing public snapshot for durable evidence. Status remains a fresh initialized-provider observation through the real public tool/socket/server path, not request-copied metadata.
- Lion explicitly superseded an intermediate sampler-offload idea with this narrower fixture-only fix. No sampler implementation/test was added. All temporary instrumentation was archived in `quality-freshness-diagnostic.patch`, then removed byte-for-byte from agent, server processing, parent communicate tests, and routing fixture before final validation.
- Production source is unchanged. Initialized route/effort/policy verification, cancellation guards, claims, persistence, and dispatch are unchanged. The regression families below exercise these existing guarantees.

## Exact validation commands

All GREEN and regression cargo commands ran in the feature checkout, with:

```text
CARGO_TARGET_DIR=/home/matcer/projects/tools/jcode-swarm-model-routing/target/stage2
CARGO_BUILD_JOBS=6
cargo test --manifest-path /home/matcer/projects/tools/jcode-swarm-model-routing/Cargo.toml -p jcode-app-core --lib <FILTER> --jobs 6 -- --nocapture --test-threads=6
```

Each subprocess bounded at180s, sequential, stop on any nonzero exit. Isolated commands add `--exact` after `--`, and use the full test name above. Each isolated invocation starts a new test process and new temporary home/repository/socket. No prior test runs in that process. Commands, exits, elapsed times and source hashes: `quality-freshness-green-results.json`, `quality-freshness-suite-results.json`.

### Three isolated GREEN observations

All three logs explicitly show **Lifecycle completed, Activity idle, Requested model gpt-5.6-sol, Requested effort high, Pinned execution and Actual execution both gpt-5.6-sol provider=openai-oauth api=openai-oauth effort=high**. Each has 1 passed, 0 failed, 0 ignored, exit0.

| Log | Observed worker | Test duration |
|---|---|---:|
| `quality-freshness-green-1.log:46-63` | `session_dove_1788900212646_2b6ff6714ef78db2` | 0.88s |
| `quality-freshness-green-2.log` | `session_stallion_1788900213859_f7083cc4ec651f60` | 0.88s |
| `quality-freshness-green-3.log` | `session_hibiscus_1788900215082_204795a01bdd3b59` | 0.86s |

### Full regression families

Each suite exits0, zero failed, zero ignored. Logs are `quality-freshness-<label>.log`. Counts overlap between filters, not unique-test totals.

| Label | FILTER | Passed |
|---|---|---:|
| communicate | `tool::communicate::` | 90 |
| routing | `routing` | 85 |
| comm-control | `server::comm_control::` | 95 |
| comm-session | `server::comm_session::` | 56 |
| client-lifecycle | `server::client_lifecycle::` | 27 |
| client-session | `server::client_session::` | 33 |
| mutation | `swarm_mutation_state` | 9 |
| persistence | `swarm_persistence_tests` | 24 |

No failed GREEN attempt or regression suite is omitted. Known unrelated unused-import warnings remain in debug_command_exec.rs/provider_control_tests.rs, plus unmatched Cargo profile warnings.

## Quality and not checked

- `quality-freshness-static-results.json`: only one source path changed, source386 LOC vs380 before, below1200. Parent communicate_tests1522 unchanged vs actual-base1870. Production diff empty, so no new production panic/swallowed/size delta. Whole-tree panic107 = actual-base107; swallowed3292 < actual-base3318, unchanged from preceding cleanup. Improvements are not attributed to this fixture fix.
- Scoped nonrecursive `rustfmt --edition 2024 --config skip_children=true --check` and `git diff --check`: exit0. Four full code-size/test-size/panic/swallowed-error scripts still exit1 on existing older-budget/base debt. Full outputs retained as `quality-freshness-{code_size,test_size,panic,swallowed_error}.log`. No budget updates or all-clean claim. The prior required-field TUI +1 exception is untouched.
- **Production sampler limitation remains**: memory-enabled real sessions can still synchronously block an async executor while sampling an embedding cache held during cold initialization. No server/base sampler fix, deterministic sampler-contention regression, or production responsiveness guarantee is claimed here.
- No live-provider heterogeneous routing, TUI binary rebuild/hash/version/runtime acceptance, full app-core/workspace suite, all-target/all-feature clippy, restart/reattach/resume, visible terminal, daemon activation, install, or reload checked. Final independent verifier owns broader acceptance. These are real public tool/server checks with an in-process provider double, not live-provider end-user acceptance.
- No global model cache was seeded, inspected for download outcome, or modified intentionally. The former fixture's unintended embedding download path is disclosed above. Existing broader suite network side effects were not instrumented.
