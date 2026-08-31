-- Demo seed schema (D-007). Dropped and recreated on each demo run.
-- Exercises: v1 type list (text, numeric, timestamptz, jsonb), PK identity,
-- FK'd concurrent-write target, and a REPLICA IDENTITY NOTHING append-only case.
DROP SCHEMA IF EXISTS app CASCADE;
CREATE SCHEMA app;

CREATE TABLE app.customers (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       text NOT NULL,
  email      text UNIQUE,
  balance    numeric(12,2) NOT NULL DEFAULT 0,
  profile    jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE app.orders (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES app.customers(id),
  status      text NOT NULL DEFAULT 'new',
  amount      numeric(12,2) NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE app.notes (
  body       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE app.notes REPLICA IDENTITY NOTHING; -- append-only eligibility (Amendment A A5.1)

INSERT INTO app.customers (name, email, balance, profile)
SELECT 'customer_' || g,
       'c' || g || '@example.com',
       ((g % 100000))::numeric / 7,
       jsonb_build_object('tier', g % 5, 'tags', jsonb_build_array('a', 'b', g % 3))
FROM generate_series(1, 5000) g;

INSERT INTO app.orders (customer_id, status, amount)
SELECT 1 + (g % 5000),
       (ARRAY['new','paid','shipped'])[1 + g % 3],
       ((g % 999900))::numeric / 100
FROM generate_series(1, 20000) g;

INSERT INTO app.notes (body)
SELECT 'note ' || g FROM generate_series(1, 3000) g;

ANALYZE app.customers;
ANALYZE app.orders;
ANALYZE app.notes;
