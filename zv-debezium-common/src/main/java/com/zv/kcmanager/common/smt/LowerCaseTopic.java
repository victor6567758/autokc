package com.zv.kcmanager.common.smt;

import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Lowers the record topic, keeping partition/key/value/timestamp/headers
 * unchanged. Example SMT for the shared SMT package; useful against sinks
 * or tooling that expect lowercase topic names.
 */
public class LowerCaseTopic<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public R apply(R record) {
        if (record.topic() == null) {
            return record;
        }
        return record.newRecord(
                record.topic().toLowerCase(Locale.ROOT),
                record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }
}