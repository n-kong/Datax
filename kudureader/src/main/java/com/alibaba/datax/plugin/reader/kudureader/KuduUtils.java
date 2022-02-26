package com.alibaba.datax.plugin.reader.kudureader;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduUtils {

    private static final Logger log = LoggerFactory.getLogger(KuduUtils.class);
    private static final ThreadLocal<KuduSession> threadLocal = new ThreadLocal<>();

    public static void main(String[] args) {
        createTable();
    }

    public static void createTable() {
        try {
            // 1、获取client
            KuduClient client = new KuduClient.KuduClientBuilder("139.64.0.218:7052").build();
            // 2、创建schema信息
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(
                new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false)
                    .build());
            columns.add(
                new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).key(false).nullable(false)
                    .build());
            columns.add(
                new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).key(false).nullable(false)
                    .build());
            Schema schema = new Schema(columns);
            // 3、指定分区字段
            List<String> partitions = Lists.newArrayList();
            partitions.add("id");
            CreateTableOptions options =
                new CreateTableOptions().addHashPartitions(partitions, 1).setNumReplicas(1);

            client.createTable("person", schema, options);
            client.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    //public static void deleteTable(String tableName) throws KuduException {
    //    KuduClient client = Kudu.INSTANCE.client();
    //    if (!client.tableExists(tableName)) {
    //        return;
    //    }
    //    client.deleteTable(tableName);
    //}
    //
    ///**
    // * 添加空列
    // *
    // * @param tableName table name
    // * @param colName column name
    // * @param type column type
    // * @throws KuduException e
    // * @throws InterruptedException i
    // */
    //public static boolean addNullableColumn(String tableName,
    //    String colName,
    //    Type type) throws KuduException, InterruptedException {
    //    if (existColumn(tableName, colName)) {
    //        log.error("表{}中的columnName: {}已存在!", tableName, colName);
    //        return false;
    //    }
    //
    //    KuduClient client = Kudu.INSTANCE.client();
    //    AlterTableOptions ato = new AlterTableOptions();
    //    ato.addNullableColumn(colName, type);
    //    client.alterTable(tableName, ato);
    //    Kudu.INSTANCE.flushTables(tableName);
    //
    //    int totalTime = 0;
    //    while (!client.isAlterTableDone(tableName)) {
    //        Thread.sleep(200);
    //        if (totalTime > 20000) {
    //            log.warn("Alter table is Not Done!");
    //            return false;
    //        }
    //        totalTime += 200;
    //    }
    //
    //    return true;
    //}
    //
    ///**
    // * 添加有默认值的列
    // */
    //public static boolean addDefaultValColumn(String tableName,
    //    String colName,
    //    Type type,
    //    Object defaultVal) throws KuduException, InterruptedException {
    //    if (existColumn(tableName, colName)) {
    //        log.error("表{}中的columnName: {}已存在!", tableName, colName);
    //        return false;
    //    }
    //
    //    KuduClient client = Kudu.INSTANCE.client();
    //    AlterTableOptions ato = new AlterTableOptions();
    //    ato.addColumn(colName, type, defaultVal);
    //    client.alterTable(tableName, ato);
    //    Kudu.INSTANCE.flushTables(tableName);
    //
    //    int totalTime = 0;
    //    while (!client.isAlterTableDone(tableName)) {
    //        Thread.sleep(200);
    //        if (totalTime > 20000) {
    //            log.warn("Alter table is Not Done!");
    //            return false;
    //        }
    //        totalTime += 200;
    //    }
    //
    //    return true;
    //}
    //
    //public static boolean existColumn(String tableName, String colName) throws KuduException {
    //    Kudu.INSTANCE.flushTables(tableName);
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //
    //    Schema schema = ktable.getSchema();
    //    List<ColumnSchema> columns = schema.getColumns();
    //    for (ColumnSchema c : columns) {
    //        if (c.getName().equals(colName)) {
    //            return true;
    //        }
    //    }
    //
    //    return false;
    //}
    //
    //public static void insert(String tableName, JSONObject data) throws KuduException {
    //    Insert insert = createInsert(tableName, data);
    //
    //    KuduSession session = getSession();
    //    session.apply(insert);
    //    session.flush();
    //
    //    closeSession();
    //}
    //
    //public static void upsert(String tableName, JSONObject data) throws KuduException {
    //    Upsert upsert = createUpsert(tableName, data);
    //    KuduSession session = getSession();
    //    session.apply(upsert);
    //    session.flush();
    //
    //    closeSession();
    //}
    //
    //public static void update(String tableName, JSONObject data) throws KuduException {
    //    Update update = createUpdate(tableName, data);
    //    KuduSession session = getSession();
    //    session.apply(update);
    //    session.flush();
    //
    //    closeSession();
    //}
    //
    ///**
    // * @param tableName 表名
    // * @param selectColumnList 查询字段名 为空时返回全部字段
    // * @param columnCondList 条件列 可为空
    // * @return data
    // * @throws KuduException k
    // */
    //public static List<JSONObject> query(String tableName,
    //    List<String> selectColumnList,
    //    List<ColumnCond> columnCondList) throws KuduException {
    //    List<JSONObject> dataList = Lists.newArrayList();
    //
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    if (null == ktable) {
    //        return null;
    //    }
    //
    //    if (null == selectColumnList) {
    //        selectColumnList = Lists.newArrayList();
    //    }
    //    if (selectColumnList.size() < 1) {
    //        selectColumnList = getColumnList(ktable);
    //    }
    //
    //    KuduScanner.KuduScannerBuilder kuduScannerBuilder =
    //        Kudu.INSTANCE.client().newScannerBuilder(ktable);
    //    kuduScannerBuilder.setProjectedColumnNames(selectColumnList);
    //
    //    /*
    //     * 设置搜索的条件 where 条件过滤字段名
    //     * 如果不设置，则全表扫描
    //     */
    //    if ((null != columnCondList) && (columnCondList.size() > 0)) {
    //        KuduPredicate predicate;
    //        for (ColumnCond cond : columnCondList) {
    //            predicate = getKuduPredicate(ktable, cond);
    //            if (null != predicate) {
    //                kuduScannerBuilder.addPredicate(predicate);
    //            }
    //        }
    //    }
    //
    //    KuduScanner scanner = kuduScannerBuilder.build();
    //    while (scanner.hasMoreRows()) {
    //        RowResultIterator rows = scanner.nextRows();
    //
    //        // 每次从tablet中获取的数据的行数, 如果查询不出数据返回0
    //        int numRows = rows.getNumRows();
    //        if (numRows > 10000) {
    //            log.error("查询数据条数: {}, 大于10000条, 数据量过载!", numRows);
    //            break;
    //        }
    //
    //        while (rows.hasNext()) {
    //            dataList.add(getRowData(rows.next(), selectColumnList));
    //        }
    //    }
    //
    //    //7、关闭client
    //    Kudu.INSTANCE.client().close();
    //
    //    return dataList;
    //}
    //
    //public static void delete(String tableName, JSONObject data) throws KuduException {
    //    Delete delete = createDelete(tableName, data);
    //    KuduSession session = getSession();
    //
    //    session.apply(delete);
    //    session.flush();
    //    closeSession();
    //
    //    Kudu.INSTANCE.client().close();
    //}
    //
    //private static Insert createInsert(String tableName, JSONObject data) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //
    //    Insert insert = ktable.newInsert();
    //    PartialRow row = insert.getRow();
    //
    //    Schema schema = ktable.getSchema();
    //    for (String colName : data.keySet()) {
    //        ColumnSchema colSchema = schema.getColumn(colName);
    //        fillRow(row, colSchema, data);
    //    }
    //
    //    return insert;
    //}
    //
    //private static Insert createEmptyInsert(String tableName) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    return ktable.newInsert();
    //}
    //
    //private static Upsert createUpsert(String tableName, JSONObject data) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //
    //    Upsert upsert = ktable.newUpsert();
    //    PartialRow row = upsert.getRow();
    //    Schema schema = ktable.getSchema();
    //    for (String colName : data.keySet()) {
    //        ColumnSchema colSchema = schema.getColumn(colName);
    //        fillRow(row, colSchema, data);
    //    }
    //
    //    return upsert;
    //}
    //
    //private static Upsert createEmptyUpsert(String tableName) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    return ktable.newUpsert();
    //}
    //
    //private static Update createUpdate(String tableName, JSONObject data) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //
    //    Update update = ktable.newUpdate();
    //    PartialRow row = update.getRow();
    //    Schema schema = ktable.getSchema();
    //    for (String colName : data.keySet()) {
    //        ColumnSchema colSchema = schema.getColumn(colName);
    //        fillRow(row, colSchema, data);
    //    }
    //
    //    return update;
    //}
    //
    //private static Update createEmptyUpdate(String tableName) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    return ktable.newUpdate();
    //}
    //
    //private static Delete createDelete(String tableName, JSONObject data) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    Delete delete = ktable.newDelete();
    //    PartialRow row = delete.getRow();
    //
    //    Schema schema = ktable.getSchema();
    //    for (String colName : data.keySet()) {
    //        ColumnSchema colSchema = schema.getColumn(colName);
    //        fillRow(row, colSchema, data);
    //    }
    //
    //    return delete;
    //}
    //
    //private static Delete createEmptyDelete(String tableName) throws KuduException {
    //    KuduTable ktable = Kudu.INSTANCE.getTable(tableName);
    //    return ktable.newDelete();
    //}
    //
    //private static void fillRow(PartialRow row, ColumnSchema colSchema, JSONObject data) {
    //    String name = colSchema.getName();
    //    if (data.get(name) == null) {
    //        return;
    //    }
    //    Type type = colSchema.getType();
    //    switch (type) {
    //        case STRING:
    //            row.addString(name, data.getString(name));
    //            break;
    //        case INT64:
    //        case UNIXTIME_MICROS:
    //            row.addLong(name, data.getLongValue(name));
    //            break;
    //        case DOUBLE:
    //            row.addDouble(name, data.getDoubleValue(name));
    //            break;
    //        case INT32:
    //            row.addInt(name, data.getIntValue(name));
    //            break;
    //        case INT16:
    //            row.addShort(name, data.getShortValue(name));
    //            break;
    //        case INT8:
    //            row.addByte(name, data.getByteValue(name));
    //            break;
    //        case BOOL:
    //            row.addBoolean(name, data.getBooleanValue(name));
    //            break;
    //        case BINARY:
    //            row.addBinary(name, data.getBytes(name));
    //            break;
    //        case FLOAT:
    //            row.addFloat(name, data.getFloatValue(name));
    //            break;
    //        default:
    //            break;
    //    }
    //}
    //
    //private static JSONObject getRowData(RowResult row, List<String> selectColumnList) {
    //    JSONObject dataJson = new JSONObject();
    //
    //    selectColumnList.forEach(x -> {
    //        Type type = row.getColumnType(x);
    //        switch (type) {
    //            case STRING:
    //                dataJson.put(x, row.getString(x));
    //                break;
    //            case INT64:
    //            case UNIXTIME_MICROS:
    //                dataJson.put(x, row.getLong(x));
    //                break;
    //            case DOUBLE:
    //                dataJson.put(x, row.getDouble(x));
    //                break;
    //            case FLOAT:
    //                dataJson.put(x, row.getFloat(x));
    //                break;
    //            case INT32:
    //            case INT16:
    //            case INT8:
    //                dataJson.put(x, row.getInt(x));
    //                break;
    //            case BOOL:
    //                dataJson.put(x, row.getBoolean(x));
    //                break;
    //            case BINARY:
    //                dataJson.put(x, row.getBinary(x));
    //                break;
    //
    //            default:
    //                break;
    //        }
    //        ;
    //    });
    //
    //    return dataJson;
    //}
    //
    //private static KuduPredicate getKuduPredicate(KuduTable ktable, ColumnCond cond) {
    //    String colName = cond.getColName();
    //    KuduPredicate.ComparisonOp op = cond.getOp();
    //    Object objVal = cond.getValue();
    //
    //    if (objVal instanceof Boolean) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op,
    //            (boolean) cond.getValue());
    //    }
    //    if (objVal instanceof Integer) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op, ((Integer) cond.getValue()).longValue());
    //    }
    //    if (objVal instanceof Long) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op, (Long) cond.getValue());
    //    }
    //    if (objVal instanceof Short) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op, ((Short) cond.getValue()).longValue());
    //    }
    //    if (objVal instanceof Byte) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op, ((Byte) cond.getValue()).longValue());
    //    }
    //    if (objVal instanceof Float) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op,
    //            (float) cond.getValue());
    //    }
    //    if (objVal instanceof Double) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op,
    //            (double) cond.getValue());
    //    }
    //    if (objVal instanceof String) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op,
    //            (String) cond.getValue());
    //    }
    //    if (objVal instanceof byte[]) {
    //        return KuduPredicate.newComparisonPredicate(
    //            ktable.getSchema().getColumn(colName),
    //            op,
    //            (byte[]) cond.getValue());
    //    }
    //
    //    return null;
    //}
    //
    //private static KuduSession getSession() throws KuduException {
    //    KuduSession session = threadLocal.get();
    //    if (session == null) {
    //        session = Kudu.INSTANCE.newSession();
    //        threadLocal.set(session);
    //    }
    //    return session;
    //}
    //
    //private static KuduSession getAsyncSession() {
    //    KuduSession session = threadLocal.get();
    //    if (session == null) {
    //        session = Kudu.INSTANCE.newAsyncSession();
    //        threadLocal.set(session);
    //    }
    //    return session;
    //}
    //
    //private static void closeSession() {
    //    KuduSession session = threadLocal.get();
    //    threadLocal.set(null);
    //    Kudu.INSTANCE.closeSession(session);
    //}
    //
    //private static List<String> getColumnList(KuduTable ktable) {
    //    List<String> columns = Lists.newArrayList();
    //
    //    List<ColumnSchema> columnSchemaList = ktable.getSchema().getColumns();
    //    columnSchemaList.forEach(x -> {
    //        columns.add(x.getName());
    //    });
    //
    //    return columns;
    //}
}
