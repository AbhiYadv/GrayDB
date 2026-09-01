SELECT customer_id, sum(amount_cents), count(*)
FROM r1.orders
WHERE created_at >= :window_end - interval '7 days'
GROUP BY customer_id;
