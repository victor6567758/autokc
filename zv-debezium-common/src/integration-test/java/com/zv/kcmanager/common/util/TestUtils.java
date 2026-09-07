package com.zv.kcmanager.common.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import org.awaitility.Awaitility;

/**
 * Small JDBC / async helpers shared by the zv-debezium integration tests,
 * in the spirit of streamkap's {@code TestUtils}
 * ({@code runSQL} / {@code querySQL} / {@code assertSQL}).
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestUtils {

    public static void runSQL(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public static List<String[]> querySQL(Connection connection, String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<String[]> rows = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = resultSet.getString(i + 1);
                }
                rows.add(row);
            }
            return rows;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    /** Asserts that {@code sql} returns exactly one row equal to {@code expectedRow}. */
    public static void assertSQL(Connection connection, String sql, String[] expectedRow) {
        assertThat(querySQL(connection, sql))
                .as(sql)
                .containsExactly(expectedRow);
    }

    /** Asserts the exact ordered result rows of {@code sql}. */
    public static void assertSQLRows(Connection connection, String sql, String[]... expectedRows) {
        assertThat(querySQL(connection, sql))
                .as(sql)
                .containsExactly(expectedRows);
    }

    /** Awaitility wrapper with a short poll interval and a readable alias in failure messages. */
    public static void await(Duration timeout, String description, Callable<Boolean> condition) {
        Awaitility.await(description)
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(200))
                .until(condition);
    }
}