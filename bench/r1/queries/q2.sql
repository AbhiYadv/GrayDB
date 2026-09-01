SELECT status, count(*)
FROM r1.orders
WHERE tenant_id = :tenant_id
GROUP BY status;
