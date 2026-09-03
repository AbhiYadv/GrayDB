-- ClickHouse CDC representation for R1-P1-v1 (spec section 10). One immutable
-- version row per PostgreSQL change; exact-at-LSN reads reduce to the greatest
-- _version with _source_lsn <= target before filtering tombstones. The complete
-- version is part of every sorting key, so background merges cannot replace an
-- older source version; no TTL or cleanup removes history during a run.

CREATE DATABASE IF NOT EXISTS r1_meta;

CREATE TABLE IF NOT EXISTS r1_tenants_raw
(
    tenant_id Int64,
    region Nullable(String),
    plan Nullable(String),
    created_at Nullable(DateTime64(6)),
    settings Nullable(String),
    _source_lsn UInt64,
    _change_ordinal UInt32,
    _version UInt128,
    _deleted UInt8
)
ENGINE = MergeTree
ORDER BY (tenant_id, _version)
SETTINGS non_replicated_deduplication_window = 1000000;

CREATE TABLE IF NOT EXISTS r1_customers_raw
(
    customer_id Int64,
    tenant_id Nullable(Int64),
    segment Nullable(String),
    email_domain Nullable(String),
    profile Nullable(String),
    created_at Nullable(DateTime64(6)),
    _source_lsn UInt64,
    _change_ordinal UInt32,
    _version UInt128,
    _deleted UInt8
)
ENGINE = MergeTree
ORDER BY (customer_id, _version)
SETTINGS non_replicated_deduplication_window = 1000000;

CREATE TABLE IF NOT EXISTS r1_orders_raw
(
    order_id Int64,
    tenant_id Nullable(Int64),
    customer_id Nullable(Int64),
    status Nullable(String),
    channel Nullable(String),
    amount_cents Nullable(Int64),
    created_at Nullable(DateTime64(6)),
    updated_at Nullable(DateTime64(6)),
    attributes Nullable(String),
    _source_lsn UInt64,
    _change_ordinal UInt32,
    _version UInt128,
    _deleted UInt8
)
ENGINE = MergeTree
ORDER BY (order_id, _version)
SETTINGS non_replicated_deduplication_window = 1000000;

CREATE TABLE IF NOT EXISTS r1_order_events_raw
(
    event_id Int64,
    order_id Nullable(Int64),
    tenant_id Nullable(Int64),
    event_type Nullable(String),
    event_at Nullable(DateTime64(6)),
    metadata Nullable(String),
    _source_lsn UInt64,
    _change_ordinal UInt32,
    _version UInt128,
    _deleted UInt8
)
ENGINE = MergeTree
ORDER BY (event_id, _version)
SETTINGS non_replicated_deduplication_window = 1000000;

CREATE TABLE IF NOT EXISTS r1_meta.applied_transactions
(
    operation_sha256 String,
    source_lsn UInt64,
    applied_at DateTime
)
ENGINE = MergeTree
ORDER BY operation_sha256
SETTINGS non_replicated_deduplication_window = 1000000;

-- Idle-stream visibility proof: the CDC loop records the last keepalive end
-- LSN when no publication changes are pending.  applied_lsn takes the
-- greatest of this and the applied transactions, so an engine that has
-- applied every change is visible at any later WAL position.
CREATE TABLE IF NOT EXISTS r1_meta.visibility
(
    source_lsn UInt64,
    recorded_at DateTime
)
ENGINE = MergeTree
ORDER BY source_lsn
SETTINGS non_replicated_deduplication_window = 1000000;
