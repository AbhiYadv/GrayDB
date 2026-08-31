# GrayDB Track A — Market-Proof Execution Kit (v1)

Ruling in force (R3): Track A opens first. S1 build begins only after two credible design-partner commitments. Kill/reposition if ten properly qualified conversations produce zero pilot commitment or reveal pipelines are considered acceptable.

Funnel gates: 40 evidence-backed targets → 10 qualified discovery calls → 5 confirm recurring expensive pain → 3 share real architecture/workload data → 2 commit to technical pilot → ≥1 identifies a budget owner.

Evidence grades: A = first-person engineering evidence (own blog/talk/docs). B = strong indirect (hiring for the stack, conference talk, vendor case study naming them). C = secondary/unverified lead (needs confirmation before counting toward the 40).

---

## 1. Target list, tranche 1 (evidence-graded)

### Tier 1 — Self-managed Debezium/Kafka pipelines, first-person pain (hottest)
| Company | Evidence | Grade | Angle |
|---|---|---|---|
| Delhivery (logistics, Delhi HQ) | Engineering post on debezium.io: running Debezium+PG on RDS in production, incidents and lessons documented; author focused on CDC/warehousing | A | Warm-geography flagship. Angle: "you wrote the lessons-learned post; we're building the thing that deletes the lessons." https://debezium.io/blog/2020/02/25/lessons-learned-running-debezium-with-postgresql-on-rds/ |
| Intuit Credit Karma | "Streaming CDC at Scale" talk (Jiufeng Liu), circulating in July-2026 data-eng link roundups | B | Scale story; discovery-value even if too big to convert: what breaks at their volume. |
| Swiggy | First-person CDC architecture series on bytes.swiggy.com (tools incl. Debezium; sources incl. MySQL/DynamoDB — verify PG share) | B | India-reachable; verify PG scope on the call, not before. https://bytes.swiggy.com/architecture-of-cdc-system-a975a081691f |
| Razorpay | Hiring Senior DevOps Engineer – Kafka (Bangalore); data-warehouse engineering posts | B | Hiring = pipeline headcount budget exists. Angle: the role you're hiring is the cost we delete. |

### Tier 2 — ClickPipes/PeerDB adopters (pain proven; destination committed; discovery + displacement)
| Company | Evidence | Grade | Angle |
|---|---|---|---|
| Seemplicity (security analytics) | ClickHouse case study: tried Debezium, rejected multi-step pipeline complexity, moved to PeerDB/ClickPipes; Chief Architect quoted | A | Discovery goldmine: what did Debezium cost them; what does CH still not give (search? PG surface? freshness proof?). |
| Ashby (recruiting SaaS) | ClickPipes GA post: Director of Engineering quoted; PG complemented by CH for customer-facing dashboards | A | Ask the freshness question: how do they explain staleness windows to customers today. |
| Blacksmith (CI/observability) | ClickHouse interview: PG + CH via CDC connector; terabytes queried | A | Also a channel: their customers are dev-infra teams. |
| Cresta (contact-center AI) | ClickHouse blog: migrated PG analytics to CH as primary warehouse | A | Post-migration retrospective discovery. |
| PeerDB early-customer class | PeerDB Cloud sunset July 30 2025; customers transitioned to ClickPipes | B | Sunset-driven re-evaluation moment; source via PeerDB community/Slack archives. |

### Tier 3 — PG→Elasticsearch search stacks (the search-side wedge)
| Company | Evidence | Grade | Angle |
|---|---|---|---|
| GitLab (validation + channel) | Their docs: "Elasticsearch is only ever a secondary data store... always derived again from PostgreSQL and Gitaly"; whole Sidekiq indexer + migration framework maintained | A | Too big to convert; use as thesis validation in every pitch. The *target class* is self-managed GitLab operators running their own ES clusters. |
| HighLevel (SaaS, Bangalore talk) | OpenHouse Bangalore 2025: migrated four use cases off MySQL/Elasticsearch/MongoDB to ClickHouse | B | Proves ES-displacement budgets exist; India-reachable speakers. |
| Viralo | ClickHouse newsletter: PG analytics collapsed at 10M users → migrated | B | The pre-sharding-moment persona, mid-size. |

### Tier 4 — India-reachable leads (verify before counting)
Zomato (Kafka→ClickHouse/ES per secondary writeups — C), Zepto, Meesho, Groww, CRED, Urban Company (C — all known PG+Kafka-era stacks per talks/job posts; confirm via current job postings and eng blogs before outreach).

Named, evidence-graded so far: 14 (8×A/B conversion-grade + class targets). Remaining ~26 filled via the playbook below — do not pad with grade-C names; upgrade them first.

## 2. Sourcing playbook to 40 (repeatable, evidence-first)
1. Job boards weekly sweep: postings containing (Debezium AND Elasticsearch) OR (Debezium AND ClickHouse) OR ("Kafka Connect" AND Elasticsearch AND Postgres). Every posting = grade-B target + names the hiring manager.
2. Debezium community: blog guest posts, mailing list, conference talks (Kafka Summit/Current CDC tracks) — first-person = grade A.
3. Elastic + ClickHouse case-study libraries filtered for "PostgreSQL" as source — grade B.
4. GitHub code search: public repos with docker-compose containing debezium + elasticsearch/clickhouse connectors owned by company orgs — grade B.
5. Self-managed GitLab operators with Advanced Search enabled (community forums, GitLab-CE admin threads) — grade B class.
6. HN/Reddit r/dataengineering threads on "keep Postgres and Elasticsearch in sync" — authors and commenters describing prod setups — grade A/B.
7. India channel: OpenHouse/Kafka meetup speaker lists (Bangalore/Delhi), builtin/naukri postings — reachability weighting for first calls.

## 3. Discovery call kit (the six learning objectives → questions)
Objective mapping (every call must answer all six; these decide what S1 tests):
- Search-first or analytics-first → "Of your derived stores, which one breaking pages someone at 2am — search or analytics? Which came first and why?"
- Freshness/RYW requirements → "When a user writes and immediately searches/reads a dashboard, what happens today? What staleness do you promise, and what do you actually deliver? Ever measured it?"
- Managed-cloud restrictions → "Where does the source PG live (RDS/Aurora/Cloud SQL/self-hosted)? What are you prohibited from installing on it?"
- Max acceptable source impact → "What read/WAL overhead on the primary would get a pipeline vetoed by your DBA? Has a slot ever filled your disk?"
- Does PG compatibility at the read side matter → "If search+analytics answered over the Postgres wire with your schema — joins included — what code disappears? Or do you not care about the query surface?"
- What they'd genuinely delete → "Walk me through every component between Postgres and your search/analytics results. Which would you delete tomorrow if correctness were guaranteed? Which will you never delete, and why?"
Plus the money questions: "How many engineer-hours/month does this pipeline consume? What's the annual Elastic/CH/Confluent spend? Who owns that budget?" And the Materialize/ClickPipes win-loss: "Evaluated Materialize, ClickPipes, Estuary, Airbyte? Why did/didn't they stick?"
Segment branch (learned from prospect session): if hours are LOW but bill is HIGH (high-bill/low-toil segment) — do NOT lead with ops-pain deletion; lead with bill consolidation (anchor: 20–30% of spend removed) + capability gaps (cross-shape joins, per-query freshness, one dialect). The toil pitch reads as irrelevant to this segment and costs credibility.
Disqualifiers (don't count toward the 10): no live pipeline (aspirational only); MySQL/Mongo-only sources; pipeline owned by an offshore vendor with no internal pain-holder; data volume trivially small (single-node PG query suffices).

## 4. Funnel tracker (columns for the CRM sheet)
company | tier | evidence grade + URL | source stack (exact) | destinations (ES/CH/both) | PG hosting | contact + role | outreach date | call date | six-objectives summary | pain $ estimate | pilot interest (Y/N/when) | budget owner named | next step | kill-counter status
Kill counters (live): qualified calls completed __/10 · expensive-pain confirms __/5 · architecture shares __/3 · pilot commits __/2 · budget owner __/1. Trigger check after every call; at 10 qualified calls with 0 commits → kill/reposition review, no extensions.

## 5. Outreach template (sells W2, not features)
Subject: deleting the Postgres→[Elasticsearch/ClickHouse] pipeline at [Company]
"[Name] — saw [specific evidence: your Debezium post / the Kafka DevOps role / your ClickPipes case study]. I'm building GrayDB: attach to your existing Postgres over logical replication, get search + analytics through a plain Postgres endpoint, with every query able to prove exactly which LSN it reflects — so Kafka, Debezium, and the reconciliation jobs get decommissioned, not wrapped. Before building further I'm doing 30-minute architecture conversations with teams running this exact stack — not selling; validating whether this deserves to exist. Two questions I'd ask you: what does the pipeline cost you monthly in engineer-hours, and what would you never trust a replacement with? Worth 30 minutes?"

## 6. R3 technical caution — logged as spec change
Rung-4 emergency raw append hardened (spec v0.4.1 amended): slot ack advances only past transaction-complete, checksummed, durably-stored frame prefixes that are self-describing — guaranteed by never splicing a dying session; raw capture always restarts a fresh replication session from last durable ack so PostgreSQL re-emits Relation/Type metadata. Added to S1 acceptance demos: induced decoder death → deterministic replay from durable frames, zero gap, zero duplicate.
