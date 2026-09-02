-- Q4 exact at target LSN. Reduce r1_order_events_raw, then count by event type
-- for a fixed tenant set over a 24-hour window.
SELECT event_type, count(*) AS `count(*)`
FROM
(
    SELECT
        tupleElement(_row, 2) AS tenant_id,
        tupleElement(_row, 3) AS event_type,
        tupleElement(_row, 4) AS event_at
    FROM
    (
        SELECT event_id, argMax((order_id, tenant_id, event_type, event_at, metadata, _deleted), _version) AS _row
        FROM r1_order_events_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY event_id
    )
    WHERE tupleElement(_row, 6) = 0
)
WHERE tenant_id IN (:tenant_set)
  AND event_at >= :window_end - INTERVAL 24 HOUR
GROUP BY event_type;
