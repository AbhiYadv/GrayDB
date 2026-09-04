-- Q1 exact at target LSN (spec section 11). Reduce r1_orders_raw to the greatest
-- _version per order_id among versions with _source_lsn <= target, drop
-- tombstones, then run the logical aggregation.
SELECT customer_id, sum(amount_cents) AS `sum(amount_cents)`, count(*) AS `count(*)`
FROM
(
    SELECT
        tupleElement(_row, 2) AS customer_id,
        tupleElement(_row, 5) AS amount_cents,
        tupleElement(_row, 6) AS created_at
    FROM
    (
        SELECT order_id, argMax((tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes, _deleted), _version) AS _row
        FROM r1_orders_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY order_id
    )
    WHERE tupleElement(_row, 9) = 0
)
WHERE created_at >= :window_end - INTERVAL 7 DAY
GROUP BY customer_id;
