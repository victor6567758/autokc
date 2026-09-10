package com.zv.connect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatusKeyParserTest {

    @Test
    void parsesConnectorKeys() {
        StatusKeyParser.ParsedKey key = StatusKeyParser.parse("status-connector-inventory-source");

        assertThat(key.type()).isEqualTo(StatusKeyParser.KeyType.CONNECTOR);
        assertThat(key.connector()).isEqualTo("inventory-source");
        assertThat(key.taskId()).isEqualTo(-1);
    }

    @Test
    void parsesTaskKeysWithDashedConnectorNames() {
        // task number = segment after the LAST dash - connector names contain dashes
        StatusKeyParser.ParsedKey key = StatusKeyParser.parse("status-task-inventory-source-0");

        assertThat(key.type()).isEqualTo(StatusKeyParser.KeyType.TASK);
        assertThat(key.connector()).isEqualTo("inventory-source");
        assertThat(key.taskId()).isZero();

        StatusKeyParser.ParsedKey multiDigit = StatusKeyParser.parse("status-task-orders-src-12");
        assertThat(multiDigit.connector()).isEqualTo("orders-src");
        assertThat(multiDigit.taskId()).isEqualTo(12);
    }

    @Test
    void ignoresNonStatusAndMalformedKeys() {
        // status-topic-* (connector topic/count statuses) is not health-relevant
        assertThat(StatusKeyParser.parse("status-topic-inventory-source-0")).isNull();
        assertThat(StatusKeyParser.parse("status-connector-")).isNull(); // blank name
        assertThat(StatusKeyParser.parse("status-task-foo")).isNull(); // no task number
        assertThat(StatusKeyParser.parse("status-task-foo-bar")).isNull(); // non-numeric task number
        assertThat(StatusKeyParser.parse("anything-else")).isNull();
        assertThat(StatusKeyParser.parse(null)).isNull();
    }
}
