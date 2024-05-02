package com.bearingpoint.dbtz;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Calendar;

record Data(
        int id,

        Timestamp timestampTz,
        Timestamp timestampWithTz,
        Timestamp timestamp,
        Timestamp timestampWithoutTz,

        Timestamp timestampTzWithCal,
        Timestamp timestampWithTzWithCal,
        Timestamp timestampWithCal,
        Timestamp timestampWithoutTzWithCal,

        OffsetDateTime odtTz,
        OffsetDateTime odtWithTz,
        OffsetDateTime odt,
        OffsetDateTime odtWithoutTz,

        LocalDateTime ldt,
        LocalDateTime ldtWithoutTz,

        String info
) {

    public static Data fromResultSet(ResultSet rs, Calendar calendar) throws SQLException {
        return new Data(
                rs.getInt("id"),

                rs.getTimestamp("data_timestamptz"),
                rs.getTimestamp("data_timestamp_with_tz"),
                rs.getTimestamp("data_timestamp"),
                rs.getTimestamp("data_timestamp_without_tz"),

                rs.getTimestamp("data_timestamptz", calendar),
                rs.getTimestamp("data_timestamp_with_tz", calendar),
                rs.getTimestamp("data_timestamp", calendar),
                rs.getTimestamp("data_timestamp_without_tz", calendar),

                rs.getObject("data_timestamptz", OffsetDateTime.class),
                rs.getObject("data_timestamp_with_tz", OffsetDateTime.class),
                rs.getObject("data_timestamp", OffsetDateTime.class),
                rs.getObject("data_timestamp_without_tz", OffsetDateTime.class),

                // data_timestamptz       can't be converted to LocalDateTime
                // data_timestamp_with_tz can't be converted to LocalDateTime
                rs.getObject("data_timestamp", LocalDateTime.class),
                rs.getObject("data_timestamp_without_tz", LocalDateTime.class),

                rs.getString("info"));
    }

}
