# OffsetProof — Engineering Journey

> A running log of every material decision: what was chosen, what was
> rejected, what evidence supported it, and what remains unverified.
> Newest entries go on top. Nothing here is a claim of completion —
> `docs/STATUS.md` tracks gate verification separately.

## 2026-11-24 — Session 1: foundations, versions, uniqueness

### 1. What the user asked for (and how I interpreted it)

Build the **full A–E product** (user chose full scope over A–C core or A–B
slice), running as a **local Docker lab** on this laptop, published as a new
**public repo named `offsetproof`**. Cross-cutting demands:

- Use Context7; use the "ponytail" skill; use the best available frontend
  skills if a frontend is built.
- Deep, broad, rigorous; every engineering decision shown.
- Maintain this `journey.md`.
- Untangle the codebase (today: 4 markdown docs, zero code — so the
  "tangle" is ambiguity, not code; resolved by scaffolding a clean layout).
- Real engineering git history with periodic commits.
- Research modern problems; find where this project uniquely helps.

### 2. Tool availability — honest inventory (no pretending)

| Requested capability | Status in this session | Substitute / action |
|---|---|---|
| Context7 (version-pinned library docs) | **No Context7 tool endpoint is installed in this session** | Used the same grounding method by hand: Docker Hub tag API + official Apache/Confluent docs, digests recorded below. If you have a Context7 MCP, point me at its config and I will re-verify through it. |
| "ponytail" skill | **Not found.** Searched `~/.claude/skills`, `~/.agents/skills`, synced/trash manifests, and the workspace — no match for `*pony*`/`*tail*`. | **Need from you: the path or install source of the ponytail skill.** I will adopt it the moment I can read its `SKILL.md`. |
| Frontend skills | `web-artifacts-builder` (React/Tailwind/shadcn) and `built-in-browser` are installed; bundled `taste` skill governs visual design and is mandatory preflight before Gate E UI work | Gate E will load `taste` first, then build the minimal read-only view. |
| `scientific-brainstorming` | Loaded and used for the uniqueness/impact track (§5). | Cited in release notes if it shapes the final report (per its license). |
| Python interpreter | **None visible**: `py` launcher reports "No installed Python found", `where.exe python` finds nothing, and `AppData\Local\Programs` is not listable from this sandbox. Python is very likely installed outside the sandbox's view. | **Need from you: either the Python exe path, or install 3.12+.** All code below is written but **NOT RUN** until then. |
| Docker daemon | Client 29.7.2 present, **daemon not running** (`docker version` cannot reach `npipe:////./pipe/docker_engine`). | **Need from you: start Docker Desktop**, then I run `make up` + integration tests. |
| `gh` auth | Installed (2.100.0) but its config is unreadable from this sandbox, so I cannot confirm login. | Local git history starts now; repo creation/push the moment auth is confirmed. Fallback: I give you the 3 commands to run. |

### 3. Version pins — verified today, not copied from the PRD

- **Kafka: `apache/kafka:4.1.2`** (PRD's candidate), verified live via the
  Docker Hub tag API on 2026-11-24: `tag_status: active`, multi-arch
  (amd64 digest `sha256:d50ab7b5…`, arm64 `sha256:4ee934b5…`), pushed
  2026-03-17. Full index digest `sha256:5cc2a2fd…`.
- **Why not 4.4.0?** Verified it exists (active, pushed 2026-10-22, index
  digest `sha256:833b867a…`, amd64 `sha256:76af3d01…`). Rejected as the
  pinned base *for now*: one month of soak is too little for a reliability
  lab's foundation, and the 4.4 line introduces preview-stage features.
  Recorded as the documented upgrade trial after Gate A is green.
- **Python client: `confluent-kafka==2.16.1`** (stable 2.x line, published
  2026-10-28, librdkafka 2.16.0). Rejected the 4.4 client: it is an explicit
  **Preview** with changed class names/imports (Rust core) — wrong base for
  reproducible work. Rejected `kafka-python`: pure-Python, throughput-limited.
- **Python: 3.12** (PRD floor; best wheel coverage for `confluent-kafka`).
- **PostgreSQL: `postgres:16`** — digest to be recorded at first pull
  (daemon unavailable today, so no silent invention).

### 4. Architecture decisions (Gate A shape)

- **Consumer offset discipline:** `enable.auto.commit=false`,
  `enable.auto.offset.store=false`; commit a message's offset **only after**
  its PostgreSQL transaction commits (or after a validated DLQ handoff).
  Ordering within a partition is never skipped past unfinished work.
- **Idempotence, not exactly-once:** one Postgres transaction inserts
  `event_id` under a unique constraint **and** applies the state transition.
  Redelivery → constraint hit → harmless no-op. The DLQ handoff explicitly
  does **not** claim exactly-once (a crash mid-handoff can duplicate).
- **Event key = `order_id`** → per-order partition affinity and ordering.
- **Single coherent workload** (synthetic order fulfillment) — no Spark,
  Hadoop, K8s, microservice sprawl. One pipeline, truthfully measured.
- **Ground-truth isolation:** the producer's ack manifest and fault labels
  live outside the investigator's input path from day one (directory-level
  separation + an import guard test), because retrofitting blindness later
  never works.

### 5. Modern-problems research → uniqueness and impact

Researched 2026 literature *after* an independent problem framing (per the
brainstorming protocol: ideas first, literature second).

**What the literature says (located evidence, 2026):**

1. **The dual-write problem is still the #1 trap.** Fresh 2026 guides
   (transactional-outbox pattern coverage across Spring/Kafka stacks) repeat
   the same lesson: Kafka transactions are atomic *inside Kafka only*; the
   moment a consumer writes to Postgres, you are in at-least-once territory
   and must design idempotence. Interviewers still use the
   crash-between-DB-commit-and-offset-commit scenario as a filter.
2. **Lag is a symptom, not a diagnosis.** Runbooks and 2025 postmortems
   converge on five causes (slow sink, hot partition, rebalance storm,
   poison pill, under-provisioning). Existing open tools (`klag`, AKHQ,
   Conduktor) classify the *symptom*. None verify *business-data
   correctness* (did order #4127 survive?) and none blind-test an LLM
   diagnostician against a rule baseline.
3. **"Just add consumers" is folk medicine.** Multiple 2025–26 analyses show
   a single hot partition caps group throughput no matter the fleet size —
   yet teams keep scaling horizontally first.
4. **Exactly-once confusion persists.** 2026 explainers still have to clarify
   that broker-side EOS ≠ end-to-end EOS with an external sink.

**Where OffsetProof fits (ideas, to be validated by the build):**

- **I1 — Correctness oracle as first-class output.** The independent
  acknowledged-manifest reconciliation (`valid_missing`,
  `duplicate_database_effects`) answers the question dashboards cannot:
  *did we lose or double-apply business effects?* F6 (crash after DB commit)
  turns the interview-filter scenario into a runnable regression test.
- **I2 — Blinded diagnosis benchmark with `unknown` as a passing grade.**
  Rules vs. LangGraph investigator on frozen, held-out fault runs, scored on
  top-1 family accuracy *and* evidence-support precision (every factual
  claim must cite timestamped telemetry). Publishing a negative result
  (rules ≥ agent) is an explicit accepted outcome.
- **I3 — Falsifiable ops claims.** F4 (hot partition) is designed to show
  scaling failing live; F2 isolates sink latency from broker latency with
  measured histograms. Each claim ships with seed, hardware, and raw bundle.

**Adversarial review (of my own framing):** the "uniqueness" above is
relative to *open, reproducible* tooling — commercial platforms may do parts
of this behind closed doors, and I have not surveyed them; the honest claim
is "reproducible + blinded + correctness-checked," not "first ever." The
`unknown`-is-correct grading needs careful control design or it becomes an
excuse for a weak baseline — tracked as a risk in `docs/STATUS.md`.

### 6. Repo and history plan

- Local repo at `offsetproof/` (this directory), git-initialized, committed
  at meaningful gates (scaffold → Gate A code → oracle/tests → B → C → D →
  E), each commit a reviewable unit. Remote `offsetproof` (public) created
  once `gh` auth is confirmed; until then the local history is the record.
- Every metric-bearing claim lands in `results/<run_id>/` with manifest,
  seed, code SHA, and raw artifacts — or it is marked NOT RUN.

## Log (newest first)

- **2026-11-24 — S10.** First live agent comparison (n=1, poison run):
  rules said ambiguous/low (correct — one post-drain snapshot cannot
  convict); gpt-4o-mini said stalled_consumer/high (wrong, overconfident).
  Both cited real telemetry, so the failure is interpretation, not
  fabrication — the publishable negative result the PRD pre-registered.
  Engineering consequence: bundles must sample DURING drains, not once
  after; single-snapshot diagnosis is the identified weakness on both
  sides. No metric was edited, hidden, or re-run to look better.
- **2026-11-24 — S9.** Provider keys wired to the user's names
  (OPENAI_API_KEY / ANTHROPIC_API_KEY, values never touched); agent mode
  now runs the real pipeline and writes incident_agent.json. History
  purged with filter-branch (21 commits kept, 313→242 objects, old blob
  IDs gone) and force-pushed — decision: a day-old sole-contributor repo
  deserves a clean past; no clones existed to break. No .env found in the
  project folders; keys remain the user's to place.
- **2026-11-24 — S7.** All fault runs green: F6 (776/776 after kill between
  DB commit and offset commit), DB-down 45s (reconnect fix proven — pre-fix
  code stranded at 439/1558), hot partition (p0 lag 1783 vs ~270, all
  reconciled), ledger (1571/1571), F5 broker kill on RF=3 (PASS, ISR
  restored), slow sink (PASS). Two aborted F5 attempts taught real lessons:
  inter-broker hostnames must be host-resolvable (per-broker host ports)
  and the producer now fails loudly on partial ack. Demo runs end to end
  (PASS/PASS, 35s) after fixing its fresh-group replay bug — groups
  continue, topics don't restart. Repo public, history pushed.
- **2026-11-24 — S8.** Demo PASS/PASS in 35s with bundles. Caught myself
  committing run artifacts AND two ground-truth files to the public repo
  (broad git add after a buggy demo run wrote under src/results/). Removed
  from tracking immediately and pushed the cleanup; blobs remain in one
  superseded commit. Impact contained: those two seeds' truth is public,
  all future evaluations use fresh seeds — and the incident is why the
  investigator never reads results/private. Lesson recorded: never broad-add
  a tree I didn't clean (git skill rule), verify `git status` names only
  intended files before every commit.
- **2026-11-24 — S5.** Poison saga closed. run-08 PASS (2911 valid +
  205/205 DLQ, zero missing/phantoms). Root causes, in order: (1) UUID-only
  DLQ column crashed on synthetic ids; (2) run-scoped oracle couldn't see
  run_id='' poison rows — fixed with envelope run_id (+ backfilled 85
  rows); (3) synthetic undecodable-* DLQ ids could never match the
  oracle's expected event_ids — fixed with envelope event_id, single-poison
  probe proved the path. run-06 evidence was destroyed by my own mid-run
  TRUNCATE (offsets already committed) — documented as operator error, not
  pipeline failure; fixture now deletes only its own rows. Subagent built
  Gate C rule engine in parallel (9 tests, green). Finance + real-data
  tracks live. User granted blanket autonomous permission; sole-contributor
  rule unchanged.
- **2026-11-24 — S6.** Solo sprint (subagent runtime down): Gate D
  read-only investigator (advisory LLM, citation validation, unknown
  fallback) + Gate E minimal web view (taste preflight, no decoration) +
  fault-runner CLI + blinded comparison scoring + bundle/benchmark CLIs.
  Rejected a guessed Admin API from memory; verified every call against the
  installed client. Unit suite at 47, ruff + strict mypy clean. README now
  reports only measured runs. No compromises taken on the way here.
- **2026-11-24 — S4.** F1 crash/restart PASS (3095/3095 after SIGKILL).
  run-02 taught a real lesson: same `order-{i}` ids across runs collided in
  the long-lived projection and failed the oracle correctly — fixed with
  per-run namespaces. Finance track: added synthetic payment-ledger profile
  (INITIATED→AUTHORIZED→CAPTURED→SETTLED, REVERSED/REFUNDED, double-settle
  rejected). Real-data track: imported UCI Online Retail II (1M real
  transactions, CC BY 4.0, SHA256 recorded); 3000-invoice slice ran
  2502 valid + 498 real cancels to DLQ. That run caught a genuine sink bug:
  rejected events left phantom rows (my earlier "cleanup" had removed the
  rollback that wasn't a no-op) — fixed with exception-driven rollback and
  three Postgres regression tests. Re-ran: PASS, zero phantoms. Oracle now
  run-scoped.
- **2026-11-24 — S3.** Sandbox installs authorized; bootstrapped Python
  without touching the system: uv binary via node egress into
  `.tools/uv-bin`, managed CPython 3.12.16 into `.tools/python`, project
  `.venv` in repo. `uv pip install -e .[dev]` succeeded. First real gates:
  **11 unit tests pass, ruff clean, mypy strict clean (15 files).**
  Toolchain then paid for itself: fixed an unused file handle, a lambda
  mypy couldn't infer, `psycopg.Json` import path, untyped callbacks,
  `None`-valued Kafka records, and bare generics. Bootstrap tooling stays
  in gitignored `.tools/`; only `.venv` layout is documented for the user.
- **2026-11-24 — S2.** Ponytail adopted (plugin `DietrichGebert/ponytail`
  5.3.2 found in Claude Code cache; main + review skills read). Ran a
  ponytail-review over the Gate A commit: caught a real oracle tautology
  (`duplicate_effects` was identically zero — replaced with
  `phantom_persisted`), a `conn.rollback()` fighting the transaction
  context manager, an unverified `cub` healthcheck, topics never created,
  and one dead import. Fixed + committed as `e99f5a9`. Tension resolved:
  ponytail governs code shape (smallest complete diff), PRD governs scope.
  Python: no registry entries, no launcher hits — no system interpreter;
  user to supply venv path or install 3.12. Docker daemon still down.
- **2026-11-24 — S1 (close).** Scaffold + Gate A code committed locally as
  `362b9a9` (scaffold) and `6a3810d` (contract/workload/producer/consumer/
  sink/oracle/tests). Nothing executed: no Python and no Docker daemon were
  reachable from this sandbox. `gh` auth unconfirmed, so no remote yet.
  Next: user starts Docker Desktop + points me at Python; then unit tests,
  `make up`, and the first real Gate A integration run.
- **2026-11-24 — S1.** Scope answers received (full A–E, local Docker,
  public `offsetproof`). Environment inventoried; three blockers named
  (Python path, Docker daemon, ponytail location). Kafka 4.1.2 / 4.4.0 and
  confluent-kafka 2.16.1 verified via primary APIs. Modern-problems research
  done; uniqueness thesis recorded (§5). Scaffold begins.
