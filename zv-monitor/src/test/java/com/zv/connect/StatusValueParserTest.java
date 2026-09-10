package com.zv.connect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatusValueParserTest {

    @Test
    void parsesRunningStatus() {
        StatusValueParser.StatusValue value = StatusValueParser.parse(
                "{\"state\":\"RUNNING\",\"worker_id\":\"worker-1:8083\",\"trace\":null,\"generation\":5}");

        assertThat(value.state()).isEqualTo("RUNNING");
        assertThat(value.workerId()).isEqualTo("worker-1:8083");
        assertThat(value.trace()).isNull();
    }

    @Test
    void parsesFailedStatusWithTrace() {
        // task values carry no "id" - the task number lives in the record key
        StatusValueParser.StatusValue value = StatusValueParser.parse(
                "{\"state\":\"FAILED\",\"worker_id\":\"worker-1:8083\","
                        + "\"trace\":\"org.apache.kafka.connect.errors.RetriableException: boom\\n\\tat X\",\"generation\":2}");

        assertThat(value.state()).isEqualTo("FAILED");
        assertThat(value.trace()).startsWith("org.apache.kafka.connect.errors.RetriableException");
    }

    @Test
    void rejectsValuesWithoutUsableState() {
        assertThat(StatusValueParser.parse("{\"worker_id\":\"w\"}")).isNull(); // no state
        assertThat(StatusValueParser.parse("{\"state\":\"\"}")).isNull(); // blank state
        assertThat(StatusValueParser.parse("{\"state\":42}")).isNull(); // non-textual
        assertThat(StatusValueParser.parse("[1,2]")).isNull(); // not an object
        assertThat(StatusValueParser.parse("not json at all")).isNull(); // garbage
    }
}
