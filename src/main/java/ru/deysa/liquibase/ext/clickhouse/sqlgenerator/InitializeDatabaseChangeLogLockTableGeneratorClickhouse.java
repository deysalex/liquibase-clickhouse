package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InitializeDatabaseChangeLogLockTableGeneratorClickhouse extends InitializeDatabaseChangeLogLockTableGenerator{
	
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
	
    @Override
    public Sql[] generateSql(InitializeDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableName = new TableNameProvider(database).getLogLockTableName();

        RawSqlStatement deleteStatement = new RawSqlStatement("ALTER TABLE " + tableName + " DELETE WHERE 1=1");
        RawSqlStatement insertStatement = new RawSqlStatement("INSERT INTO " + tableName + " (ID, LOCKED) VALUES (1, 0)");

    	List<Sql> sql = new ArrayList<>();
        sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(deleteStatement, database)));
        sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(insertStatement, database)));
        return sql.toArray(new Sql[0]);
    }

}
