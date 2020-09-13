package ru.deysa.liquibase.ext.clickhouse.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ClickhouseDatabase extends AbstractJdbcDatabase {
	public static final String PRODUCT_NAME = "Clickhouse";
	public static final String DEFAULT_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

	@Override
	public String getShortName() {
		return "clickhouse";
	}

	public ClickhouseDatabase() {
		setDefaultSchemaName("default");
	}

	@Override
	public int getPriority() {
		return PRIORITY_DEFAULT;
	}

	@Override
	protected String getDefaultDatabaseProductName() {
		return "Clickhouse";
	}

	@Override
	public Integer getDefaultPort() {
		return 8123;
	}

	@Override
	public boolean supportsInitiallyDeferrableColumns() {
		return false;
	}

	@Override
	public boolean supportsSequences() {
		return false;
	}

	@Override
	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
		String databaseProductName = conn.getDatabaseProductName();
		return PRODUCT_NAME.equalsIgnoreCase(databaseProductName);
	}

	@Override
	public String getDefaultDriver(String url) {
		return DEFAULT_DRIVER_NAME;
	}

	@Override
	public boolean supportsTablespaces() {
		return false;
	}

	@Override
	public boolean supportsRestrictForeignKeys() {
		return false;
	}

	@Override
	public boolean supportsDropTableCascadeConstraints() {
		return false;
	}

	@Override
	public boolean isAutoCommit() throws DatabaseException {
		return true;
	}

	@Override
	public void setAutoCommit(boolean b) throws DatabaseException {
	}

	@Override
	public boolean isCaseSensitive() {
		return true;
	}

	@Override
	public String getCurrentDateTimeFunction() {
		return String.valueOf(System.currentTimeMillis());
	}

	public Statement getStatement() throws ClassNotFoundException, SQLException {
		String url = super.getConnection().getURL();
		Class.forName(DEFAULT_DRIVER_NAME);
		Connection con = DriverManager.getConnection(url);
		return con.createStatement();
	}

}
