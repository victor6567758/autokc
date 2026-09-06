-- zv SQL resource shipped with the connector plugin archive.
-- ${key} placeholders resolve from the connector configuration, e.g. ${slot.name}.
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as "wal_lsn_diff"
FROM pg_replication_slots
WHERE slot_name = '${slot.name}';