package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;

public class LockDatabaseChangeLogGeneratorClickhouse extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableName = new TableNameProvider(database).getLogLockTableName();
        String lockedBy = hostname + " (" + hostaddress + ")";

        RawSqlStatement updateStatement = new RawSqlStatement(
                "ALTER TABLE " + tableName +
                        " UPDATE LOCKED = 1, LOCKEDBY = '" + lockedBy + "', LOCKGRANTED = " + System.currentTimeMillis() + " WHERE ID = 1");
        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
    }
}
