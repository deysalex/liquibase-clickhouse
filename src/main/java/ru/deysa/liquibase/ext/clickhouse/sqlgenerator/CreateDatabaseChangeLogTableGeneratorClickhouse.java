package ru.deysa.liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.RawSqlStatement;
import ru.deysa.liquibase.ext.clickhouse.database.ClickhouseDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateDatabaseChangeLogTableGeneratorClickhouse extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof ClickhouseDatabase;
    }
    
    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableName = new TableNameProvider(database).getLogTableName();
        RawSqlStatement createTableStatement = new RawSqlStatement(
                "CREATE TABLE IF NOT EXISTS " + tableName +
                        " (ID String, AUTHOR String, FILENAME String, DATEEXECUTED timestamp, ORDEREXECUTED INT, " +
                        "EXECTYPE String, MD5SUM String, DESCRIPTION String, COMMENTS String, TAG String, " +
                        "LIQUIBASE String, CONTEXTS Nullable(String), LABELS Nullable(String), DEPLOYMENT_ID String) " +
                        "ENGINE = MergeTree() ORDER BY ID PRIMARY KEY ID");

        List<Sql> sql = new ArrayList<>(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database)));
        return sql.toArray(new Sql[0]);
    }
    
}
