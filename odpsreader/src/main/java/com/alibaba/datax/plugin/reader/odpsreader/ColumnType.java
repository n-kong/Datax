package com.alibaba.datax.plugin.reader.odpsreader;

public enum ColumnType {
    PARTITION, NORMAL, CONSTANT, UNKNOWN, ;

    @Override
    public String toString() {
        switch (this) {
        case PARTITION:
            return "partition";
        case NORMAL:
            return "normal";
        case CONSTANT:
            return "constant";
        default:
            return "unknown";
        }
    }

    public static ColumnType asColumnType(String columnTypeString) {
        if ("partition".equalsIgnoreCase(columnTypeString)) {
            return PARTITION;
        } else if ("normal".equalsIgnoreCase(columnTypeString)) {
            return NORMAL;
        } else if ("constant".equalsIgnoreCase(columnTypeString)) {
            return CONSTANT;
        } else {
            return UNKNOWN;
        }
    }
}
