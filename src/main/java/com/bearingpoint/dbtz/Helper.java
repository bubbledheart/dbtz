package com.bearingpoint.dbtz;

import java.sql.Connection;
import java.sql.SQLException;

public class Helper {

    public static final String DONE = "OK";

    public static final String FAILED = "FAILED";

    private final String schema;

    private final String table;

    public Helper(DatabaseConfig databaseConfig) {
        this.schema = databaseConfig.schema();
        this.table = databaseConfig.table();
    }

    public void createTable(Connection connection) throws SQLException {
        String createStatement = String.format("""
                create table %s.%s (
                    id                        integer primary key,
                    data_timestamptz          timestamptz              not null,
                    data_timestamp_with_tz    timestamp with time zone not null,
                    data_timestamp            timestamp                not null,
                    data_timestamp_without_tz timestamp                not null,
                    info                      text
                )
                """, schema, table);

        runStatement(connection, createStatement, "Creating table " + schema + "." + table + " ... ");
    }

    public void dropTable(Connection connection) throws SQLException {
        String dropStatement = String.format("drop table if exists %s.%s", schema, table);

        runStatement(connection, dropStatement, "Dropping table " + schema + "." + table + " ... ");
    }

    private static void runStatement(Connection connection, String statement, String message) throws SQLException {
        System.out.print(message);
        try (var preparedStatement = connection.prepareStatement(statement)) {
            preparedStatement.execute();
            System.out.println(DONE);
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

}
