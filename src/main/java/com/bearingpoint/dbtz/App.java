package com.bearingpoint.dbtz;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import static com.bearingpoint.dbtz.Helper.DONE;
import static com.bearingpoint.dbtz.Helper.FAILED;

public class App {

    // These patterns are chosen to be more similar to Timestamp's toString implementation for less visual clutter
    private static final DateTimeFormatter ODT_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.Sxxx");
    private static final DateTimeFormatter LDT_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.S");

    public static void main(String[] args) {

        ZoneId europeWest = ZoneId.of("Europe/London");
        ZoneId europeCentral = ZoneId.of("Europe/Vienna");
        ZoneId europeEast = ZoneId.of("Europe/Sofia");

        ZoneId usWest = ZoneId.of("America/Los_Angeles");
        ZoneId usCentralWest = ZoneId.of("America/Denver");
        ZoneId usCentralEast = ZoneId.of("America/Chicago");
        ZoneId usEast = ZoneId.of("America/New_York");

        workWithZoneIds(usWest, europeCentral);
    }

    private static void workWithZoneIds(ZoneId jvmZoneId, ZoneId appZoneId) {
        // Set the JVM's default time zone
        System.setProperty("user.timezone", jvmZoneId.getId());

        DataSource dataSource = createDatasource("127.0.0.1", 5434, "test", "postgres", "postgres");

        work(appZoneId, dataSource, "public", "test_data", true);
    }

    private static void work(ZoneId zoneId, DataSource dataSource, String schema, String table, boolean dropTableAfterFinish) {
        String insert = String.format("insert into %s.%s(id, data_timestamp, data_timestamptz, data_timestamp_with_tz, data_timestamp_without_tz, info) values (?, ?, ?, ?, ?, ?)", schema, table);
        String select = String.format("select id, data_timestamp, data_timestamptz, data_timestamp_with_tz, data_timestamp_without_tz, info from %s.%s", schema, table);

        ZonedDateTime zdt = ZonedDateTime.of(2013, 9, 13, 9, 0, 0, 0, zoneId);

        OffsetDateTime odt = zdt.toOffsetDateTime();
        LocalDateTime ldt = zdt.toLocalDateTime();
        Instant instant = zdt.toInstant();

        Timestamp timestamp = Timestamp.from(instant);
        Calendar tzUTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        System.out.println("JVM's default zoneId: " + ZoneId.systemDefault());
        try (Connection conn = dataSource.getConnection()) {
            System.out.println("Database/JDBC zoneId: " + readDatabaseTimezone(conn) + " (usually matches JVM's default zoneId)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Chosen zoneId:        " + zoneId);

        System.out.println();
        System.out.println("Instant:              " + instant + "  <-- this (or its equivalent) is what we want again after writing and reading");
        System.out.println();
        System.out.println("ZonedDateTime:        " + zdt);
        System.out.println("OffsetDateTime:       " + odt);
        System.out.println("LocalDateTime:        " + ldt);
        System.out.println();
        System.out.println("Timestamp (legacy):   " + timestamp);
        System.out.println();
        System.out.println("------------------------------------------------------------------------------------------------------------------------");
        System.out.println();

        final List<Data> data;

        try (Connection conn = dataSource.getConnection()) {
            Helper helper = new Helper(schema, table);

            helper.dropTable(conn);
            helper.createTable(conn);

            System.out.println();

            insertDataAsTimestamp(conn, insert, timestamp);
            insertDataAsTimestampWithCalendar(conn, insert, timestamp, tzUTC);
            insertDataAsOffsetDateTime(conn, insert, odt);
            insertDataAsLocalDateTime(conn, insert, ldt);

            System.out.println();

            data = readData(conn, select, tzUTC);

            if (dropTableAfterFinish) {
                System.out.println();
                helper.dropTable(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println();
        System.out.println("------------------------------------------------------------------------------------------------------------------------");
        System.out.println();
        System.out.println("<id> (Inserted as <Java data type>)");
        System.out.println();
        System.out.println("    <DB data type>              read as <Java data type>        = <Formatted Java data type>  (<equivalent Instant>)   ?");

        data.forEach(x -> {
            String s = x.id() + " (" + x.info() + ")\n" +
                    "\n    timestamptz                 read as Timestamp               = " + getString(instant, x.timestampTz()) +
                    "\n    timestamptz                 read as Timestamp with Calendar = " + getString(instant, x.timestampTzWithCal()) +
                    "\n    timestamptz                 read as OffsetDateTime          = " + getString(instant, x.odtTz()) +
                    "\n" +
                    "\n    timestamp with time zone    read as Timestamp               = " + getString(instant, x.timestampWithTz()) +
                    "\n    timestamp with time zone    read as timestamp with Calendar = " + getString(instant, x.timestampWithTzWithCal()) +
                    "\n    timestamp with time zone    read as OffsetDateTime          = " + getString(instant, x.odtWithTz()) +
                    "\n" +
                    "\n    timestamp                   read as Timestamp               = " + getString(instant, x.timestamp()) +
                    "\n    timestamp                   read as Timestamp with Calendar = " + getString(instant, x.timestampWithCal()) +
                    "\n    timestamp                   read as OffsetDateTime          = " + getString(instant, x.odt()) +
                    "\n    timestamp                   read as LocalDateTime           = " + getString(instant, x.ldt()) +
                    "\n" +
                    "\n    timestamp without time zone read as Timestamp               = " + getString(instant, x.timestampWithoutTz()) +
                    "\n    timestamp without time zone read as Timestamp with Calendar = " + getString(instant, x.timestampWithoutTzWithCal()) +
                    "\n    timestamp without time zone read as OffsetDateTime          = " + getString(instant, x.odtWithoutTz()) +
                    "\n    timestamp without time zone read as LocalDateTime           = " + getString(instant, x.ldtWithoutTz());

            System.out.println();
            System.out.println(s);
        });
    }

    private static List<Data> readData(Connection conn, String select, Calendar tzUTC) throws SQLException {
        System.out.print("Reading data ... ");

        List<Data> data = new ArrayList<>();

        try (PreparedStatement preparedStatement = conn.prepareStatement(select)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    data.add(Data.fromResultSet(rs, tzUTC));
                }
            }
            System.out.println(DONE);
            return data;
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

    private static String readDatabaseTimezone(Connection conn) throws SQLException {

        // In Postgres, the result depends on the JVM running the JDBC driver which uses that JVM's time zone
        try (PreparedStatement preparedStatement = conn.prepareStatement("show timezone")) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }

        return null;
    }

    private static void insertDataAsTimestamp(Connection conn, String insert, Timestamp timestamp) throws SQLException {
        System.out.print("Inserting data as Timestamp ................. ");

        try (PreparedStatement preparedStatement = conn.prepareStatement(insert)) {
            preparedStatement.setInt(1, 1001);
            preparedStatement.setTimestamp(2, timestamp);
            preparedStatement.setTimestamp(3, timestamp);
            preparedStatement.setTimestamp(4, timestamp);
            preparedStatement.setTimestamp(5, timestamp);
            preparedStatement.setString(6, "Inserted as java.sql.Timestamp (legacy)");

            preparedStatement.executeUpdate();
            System.out.println(DONE);
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

    private static void insertDataAsTimestampWithCalendar(Connection conn, String insert, Timestamp timestamp, Calendar tzUTC) throws SQLException {
        System.out.print("Inserting data as Timestamp with Calendar ... ");

        try (PreparedStatement preparedStatement = conn.prepareStatement(insert)) {
            preparedStatement.setInt(1, 1002);
            preparedStatement.setTimestamp(2, timestamp, tzUTC);
            preparedStatement.setTimestamp(3, timestamp, tzUTC);
            preparedStatement.setTimestamp(4, timestamp, tzUTC);
            preparedStatement.setTimestamp(5, timestamp, tzUTC);
            preparedStatement.setString(6, "Inserted as java.sql.Timestamp with java.util.Calendar (legacy)");

            preparedStatement.executeUpdate();
            System.out.println(DONE);
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

    private static void insertDataAsOffsetDateTime(Connection conn, String insert, OffsetDateTime odt) throws SQLException {
        System.out.print("Inserting data as OffsetDateTime ............ ");

        try (PreparedStatement preparedStatement = conn.prepareStatement(insert)) {
            preparedStatement.setInt(1, 1003);
            preparedStatement.setObject(2, odt);
            preparedStatement.setObject(3, odt);
            preparedStatement.setObject(4, odt);
            preparedStatement.setObject(5, odt);
            preparedStatement.setString(6, "Inserted as java.time.OffsetDateTime");

            preparedStatement.executeUpdate();
            System.out.println(DONE);
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

    private static void insertDataAsLocalDateTime(Connection conn, String insert, LocalDateTime ldt) throws SQLException {
        System.out.print("Inserting data as LocalDateTime ............. ");

        try (PreparedStatement preparedStatement = conn.prepareStatement(insert)) {
            preparedStatement.setInt(1, 1004);
            preparedStatement.setObject(2, ldt);
            preparedStatement.setObject(3, ldt);
            preparedStatement.setObject(4, ldt);
            preparedStatement.setObject(5, ldt);
            preparedStatement.setString(6, "Inserted as java.time.LocalDateTime");

            preparedStatement.executeUpdate();
            System.out.println(DONE);
        } catch (SQLException e) {
            System.out.println(FAILED);
            throw e;
        }
    }

    private static String getString(Instant now, Timestamp timestamp) {
        return getString(timestamp.toString(), timestamp.toInstant(), ok(timestamp, now));
    }

    private static String getString(Instant now, OffsetDateTime offsetDateTime) {
        return getString(ODT_PATTERN.format(offsetDateTime), offsetDateTime.toInstant(), ok(offsetDateTime, now));
    }

    private static String getString(Instant now, LocalDateTime localDateTime) {
        return getString(LDT_PATTERN.format(localDateTime), localDateTime.atOffset(ZoneOffset.UTC).toInstant(), ok(localDateTime, now));
    }

    private static String getString(String string, Instant instant, boolean isOk) {
        return String.format("%-27s", string) + " (" + instant + ")   " + (isOk ? "✓" : "✘");
    }

    private static boolean ok(Timestamp timestamp, Instant expected) {
        return timestamp.toInstant().equals(expected);
    }

    private static boolean ok(OffsetDateTime offsetDateTime, Instant expected) {
        return offsetDateTime.toInstant().equals(expected);
    }

    private static boolean ok(LocalDateTime localDateTime, Instant expected) {
        return localDateTime.atOffset(ZoneOffset.UTC).toInstant().equals(expected);
    }

    private static DataSource createDatasource(String host, int port, String database, String username, String password) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setLocalSocketAddress(host);
        dataSource.setPortNumbers(new int[]{port});
        dataSource.setDatabaseName(database);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        return dataSource;
    }

}

