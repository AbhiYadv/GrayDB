-- Q2 exact at target LSN. Reduce r1_orders_raw, then count by status for one tenant.
SELECT status, count(*) AS `count(*)`
FROM
(
    SELECT
        tupleElement(_row, 1) AS tenant_id,
        tupleElement(_row, 3) AS status
    FROM
    (
        SELECT order_id, argMax((tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes, _deleted), _version) AS _row
        FROM r1_orders_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY order_id
    )
    WHERE tupleElement(_row, 9) = 0
)
WHERE tenant_id = :tenant_id
GROUP BY status;
