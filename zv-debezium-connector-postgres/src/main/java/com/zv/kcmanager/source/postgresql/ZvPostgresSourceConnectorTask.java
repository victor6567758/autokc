package com.zv.kcmanager.source.postgresql;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorTask;


public class ZvPostgresSourceConnectorTask extends PostgresConnectorTask {

    private volatile SqlStatementExecutor sqlExecutor;

    @Override
    public CdcSourceTaskContext<PostgresConnectorConfig> preStart(Configuration config) {
        CdcSourceTaskContext<PostgresConnectorConfig> context = super.preStart(config);
        if (sqlExecutor != null) {
            sqlExecutor.close();
        }
        sqlExecutor = SqlStatementExecutor.startIfEnabled(config);
        return context;
    }

    @Override
    protected void doStop() {
        SqlStatementExecutor executor = sqlExecutor;
        sqlExecutor = null;
        if (executor != null) {
            executor.close();
        }
        super.doStop();
    }
}