-- Q5 exact at target LSN. Reduce r1_orders_raw (updates and deletes included),
-- then report current order count and amount by status.
SELECT status, count(*) AS `count(*)`, sum(amount_cents) AS `sum(amount_cents)`
FROM
(
    SELECT
        tupleElement(_row, 3) AS status,
        tupleElement(_row, 5) AS amount_cents
    FROM
    (
        SELECT order_id, argMax((tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes, _deleted), _version) AS _row
        FROM r1_orders_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY order_id
    )
    WHERE tupleElement(_row, 9) = 0
)
GROUP BY status;
