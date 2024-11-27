package com.bearingpoint.dbtz;

public record DatabaseConfig(String host, int port, String database, String username, String password, String schema, String table, boolean dropTableAfterFinish) {

    public static final boolean DROP_TABLE_AFTER_FINISH = true;
    public static final boolean KEEP_TABLE_AFTER_FINISH = false;

}
