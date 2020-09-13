package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.RawSqlStatement;
import ru.deysa.liquibase.ext.clickhouse.database.ClickhouseDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateDatabaseChangeLogLockTableGeneratorClickhouse extends CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof ClickhouseDatabase;
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableName = new TableNameProvider(database).getLogLockTableName();
        RawSqlStatement createTableStatement = new RawSqlStatement(
                "CREATE TABLE IF NOT EXISTS " + tableName +
                        " (ID INT, LOCKED BOOLEAN, LOCKGRANTED timestamp, LOCKEDBY Nullable(String)) " +
                        "ENGINE = MergeTree() ORDER BY ID PRIMARY KEY ID");

        List<Sql> sql = new ArrayList<>(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database)));
        return sql.toArray(new Sql[0]);
    }

}
