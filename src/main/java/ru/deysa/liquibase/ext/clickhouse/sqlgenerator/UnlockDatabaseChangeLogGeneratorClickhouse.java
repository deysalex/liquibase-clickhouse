package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

public class UnlockDatabaseChangeLogGeneratorClickhouse extends UnlockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(UnlockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableName = new TableNameProvider(database).getLogLockTableName();
        RawSqlStatement releaseStatement = new RawSqlStatement("ALTER TABLE " + tableName + " UPDATE LOCKED = 0, LOCKEDBY = NULL WHERE ID = 1");
        return SqlGeneratorFactory.getInstance().generateSql(releaseStatement, database);
    }

}
