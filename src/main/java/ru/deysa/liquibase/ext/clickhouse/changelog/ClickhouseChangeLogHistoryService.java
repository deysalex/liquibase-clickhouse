package ru.deysa.liquibase.ext.clickhouse.changelog;

import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import ru.deysa.liquibase.ext.clickhouse.database.ClickhouseDatabase;

public class ClickhouseChangeLogHistoryService extends StandardChangeLogHistoryService {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof ClickhouseDatabase;
    }

    @Override
    protected String getLabelsSize() {
        return "0";
    }

    @Override
    protected String getContextsSize() {
        return "0";
    }
}
