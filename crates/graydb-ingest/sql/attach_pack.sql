-- GrayDB source attach pack — SQL objects only (I5). Idempotent: safe to re-run.
-- Installs: schema graydb, table graydb.ddl_log, three event triggers
-- (ddl_command_end, sql_drop, table_rewrite) writing normalized command records.
-- ddl_log is added to the publication by the attach code so DDL arrives LSN-ordered
-- in-stream, interleaved exactly with data (wedge spec section 4, layer 1).

CREATE SCHEMA IF NOT EXISTS graydb;

CREATE TABLE IF NOT EXISTS graydb.ddl_log (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  captured_at     timestamptz NOT NULL DEFAULT now(),
  kind            text NOT NULL,            -- 'command' | 'drop' | 'table_rewrite'
  command_tag     text,
  object_type     text,
  object_identity text,
  schema_name     text,
  ddl_text        text
);

CREATE OR REPLACE FUNCTION graydb.capture_ddl_command_end() RETURNS event_trigger
LANGUAGE plpgsql AS $gdb$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    -- D-008: skip our own schema to avoid self-noise.
    IF r.schema_name IS DISTINCT FROM 'graydb' THEN
      INSERT INTO graydb.ddl_log (kind, command_tag, object_type, object_identity, schema_name, ddl_text)
      VALUES ('command', r.command_tag, r.object_type, r.object_identity, r.schema_name, current_query());
    END IF;
  END LOOP;
END
$gdb$;

CREATE OR REPLACE FUNCTION graydb.capture_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $gdb$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
    IF r.schema_name IS DISTINCT FROM 'graydb' THEN
      INSERT INTO graydb.ddl_log (kind, command_tag, object_type, object_identity, schema_name, ddl_text)
      VALUES ('drop', TG_TAG, r.object_type, r.object_identity, r.schema_name, current_query());
    END IF;
  END LOOP;
END
$gdb$;

CREATE OR REPLACE FUNCTION graydb.capture_table_rewrite() RETURNS event_trigger
LANGUAGE plpgsql AS $gdb$
BEGIN
  INSERT INTO graydb.ddl_log (kind, command_tag, object_type, object_identity, schema_name, ddl_text)
  VALUES ('table_rewrite', TG_TAG, 'table',
          pg_event_trigger_table_rewrite_oid()::regclass::text, NULL, current_query());
END
$gdb$;

DO $gdb_do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'graydb_ddl_command_end') THEN
    CREATE EVENT TRIGGER graydb_ddl_command_end ON ddl_command_end
      EXECUTE FUNCTION graydb.capture_ddl_command_end();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'graydb_sql_drop') THEN
    CREATE EVENT TRIGGER graydb_sql_drop ON sql_drop
      EXECUTE FUNCTION graydb.capture_sql_drop();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'graydb_table_rewrite') THEN
    CREATE EVENT TRIGGER graydb_table_rewrite ON table_rewrite
      EXECUTE FUNCTION graydb.capture_table_rewrite();
  END IF;
END
$gdb_do$;
