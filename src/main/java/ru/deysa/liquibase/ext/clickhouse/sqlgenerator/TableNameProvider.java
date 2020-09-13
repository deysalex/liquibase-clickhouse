package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;

public class TableNameProvider {

    private final Database database;

    public TableNameProvider(Database database) {
        this.database = database;
    }

    public String getLogLockTableName() {
        return database.escapeTableName(
                database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(),
                database.getDatabaseChangeLogLockTableName());
    }

    public String getLogTableName() {
        return database.escapeTableName(
                database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(),
                database.getDatabaseChangeLogTableName());
    }
}
