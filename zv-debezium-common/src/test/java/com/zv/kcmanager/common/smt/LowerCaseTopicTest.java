package com.zv.kcmanager.common.smt;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class LowerCaseTopicTest {

    private final LowerCaseTopic<SourceRecord> smt = new LowerCaseTopic<>();

    @Test
    void lowercasesTheTopicAndKeepsEverythingElse() {
        Schema keySchema = SchemaBuilder.string().build();
        Schema valueSchema = SchemaBuilder.string().build();
        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "sourcedb"),
                Collections.singletonMap("lsn", 42L),
                "Sourcedb.Public.Customers",
                0,
                keySchema, "key-1",
                valueSchema, "value-1");

        SourceRecord transformed = smt.apply(record);

        assertThat(transformed.topic()).isEqualTo("sourcedb.public.customers");
        assertThat(transformed.kafkaPartition()).isEqualTo(0);
        assertThat(transformed.keySchema()).isSameAs(keySchema);
        assertThat(transformed.key()).isEqualTo("key-1");
        assertThat(transformed.valueSchema()).isSameAs(valueSchema);
        assertThat(transformed.value()).isEqualTo("value-1");
        assertThat(transformed.sourcePartition()).isEqualTo(record.sourcePartition());
        assertThat(transformed.sourceOffset()).isEqualTo(record.sourceOffset());
    }

    @Test
    void nullTopicRecordsPassThroughUnchanged() {
        SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                null,
                null, null,
                SchemaBuilder.string().build(), "value-1");

        assertThat(smt.apply(record)).isSameAs(record);
    }
}