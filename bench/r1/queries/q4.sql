SELECT event_type, count(*)
FROM r1.order_events
WHERE tenant_id IN (:tenant_set)
  AND event_at >= :window_end - interval '24 hours'
GROUP BY event_type;
