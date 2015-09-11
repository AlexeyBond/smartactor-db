-- This SQL script should be executed on postgresql database before it can be used with database access actor.

-- Function used for building index on ID field casted to jsonb
CREATE OR REPLACE FUNCTION bigint_to_jsonb_immutable(source BIGINT) RETURNS jsonb AS
$body$ BEGIN RETURN to_json(source)::jsonb; END; $body$ LANGUAGE 'plpgsql' IMMUTABLE;

-- Function used for building indexes on date/time fields.
CREATE OR REPLACE FUNCTION parse_timestamp_immutable(source JSONB) RETURNS timestamptz AS
$body$ BEGIN RETURN source::text::timestamptz; END; $body$ LANGUAGE 'plpgsql' IMMUTABLE;
