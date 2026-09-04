-- Q3 exact at target LSN. Reduce BOTH r1_orders_raw and r1_tenants_raw before
-- joining, then aggregate by region, channel, and status.
SELECT region, channel, status, sum(amount_cents) AS `sum(amount_cents)`, count(*) AS `count(*)`
FROM
(
    SELECT
        tupleElement(_row, 1) AS tenant_id,
        tupleElement(_row, 3) AS status,
        tupleElement(_row, 4) AS channel,
        tupleElement(_row, 5) AS amount_cents
    FROM
    (
        SELECT order_id, argMax((tenant_id, customer_id, status, channel, amount_cents, created_at, updated_at, attributes, _deleted), _version) AS _row
        FROM r1_orders_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY order_id
    )
    WHERE tupleElement(_row, 9) = 0
) AS o
INNER JOIN
(
    SELECT
        tenant_id,
        tupleElement(_row, 1) AS region
    FROM
    (
        SELECT tenant_id, argMax((region, plan, created_at, settings, _deleted), _version) AS _row
        FROM r1_tenants_raw
        WHERE _source_lsn <= {target_lsn}
        GROUP BY tenant_id
    )
    WHERE tupleElement(_row, 5) = 0
) AS t ON t.tenant_id = o.tenant_id
GROUP BY region, channel, status;
