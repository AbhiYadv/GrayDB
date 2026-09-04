SELECT status, count(*), sum(amount_cents)
FROM r1.orders
GROUP BY status;
