package ru.deysa.liquibase.ext.clickhouse.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class ClickhouseDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("clickhouse", new ClickhouseDatabase().getShortName());
    }

}
