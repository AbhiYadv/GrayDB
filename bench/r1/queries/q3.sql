SELECT t.region, o.channel, o.status, sum(o.amount_cents), count(*)
FROM r1.orders o JOIN r1.tenants t ON t.tenant_id = o.tenant_id
GROUP BY t.region, o.channel, o.status;
