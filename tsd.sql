CREATE TABLE IF NOT EXISTS entities (
    id BIGSERIAL PRIMARY KEY,
    entity VARCHAR(1024) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS keys (
    id BIGSERIAL PRIMARY KEY,
    key VARCHAR(1024) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS tsd (
    id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT REFERENCES entities(id),
    key_id BIGINT REFERENCES keys(id),
    added TIMESTAMP,
    value DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS tsd_added_idx ON tsd (added);
CREATE INDEX IF NOT EXISTS tsd_eid_idx ON tsd (entity_id);
CREATE INDEX IF NOT EXISTS tsd_kid_idx ON tsd (key_id);
CREATE INDEX IF NOT EXISTS entity_idx ON entities (entity);
CREATE INDEX IF NOT EXISTS key_idx ON keys (key);

CREATE OR REPLACE FUNCTION upd_added() RETURNS TRIGGER AS $ex_tbl$
BEGIN
    UPDATE tsd set added=CURRENT_TIMESTAMP where id=new.ID and added is null;
    RETURN NEW;
END;
$ex_tbl$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS key_id(varchar);
CREATE OR REPLACE FUNCTION key_id(new_key varchar(1024)) RETURNS BIGINT AS $test$
BEGIN
    INSERT INTO keys (key) VALUES (new_key) ON CONFLICT DO NOTHING;
    RETURN (SELECT id from keys where key = new_key);
END
$test$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS ent_id(varchar);
CREATE OR REPLACE FUNCTION ent_id(new_ent varchar(1024)) RETURNS BIGINT AS $etest$
BEGIN
    INSERT INTO entities (entity) VALUES (new_ent) ON CONFLICT DO NOTHING;
    RETURN (SELECT id from entities where entity = new_ent);
END
$etest$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS keys_by_ent(varchar);
CREATE OR REPLACE FUNCTION keys_by_ent(ent varchar(1024)) RETURNS TABLE(keys varchar(1024)) AS $etest$
BEGIN
    RETURN QUERY
    SELECT DISTINCT k.key FROM keys k, entities e, tsd t 
    WHERE 
        e.entity = ent
        AND e.id = t.entity_id
        AND k.id = t.key_id
        AND added > NOW() - interval '30 minutes'
    ORDER BY k.key;
END
$etest$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS histokey(varchar);
CREATE OR REPLACE FUNCTION histokey(key varchar(1024)) RETURNS varchar(1024) AS $BODY$
DECLARE
    matches text[];
BEGIN
    matches := regexp_match(key, 'histogram\.([\d\.]+)\.to\.([\d\.]+)\.[a-z]');
    return CONCAT(matches[1], '-', matches[2]);
END
$BODY$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsd_ts_upd on tsd;
CREATE TRIGGER tsd_ts_upd AFTER INSERT ON tsd
    FOR EACH ROW EXECUTE PROCEDURE upd_added();
