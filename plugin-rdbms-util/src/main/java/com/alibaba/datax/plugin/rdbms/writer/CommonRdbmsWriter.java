package com.alibaba.datax.plugin.rdbms.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonRdbmsWriter {

    public static class Job {
        private static final Logger LOG = LoggerFactory
            .getLogger(Job.class);
        private DataBaseType dataBaseType;

        public Job(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
            OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
        }

        public void init(Configuration originalConfig) {
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
                originalConfig.toJSON());
        }

        /*目前只支持MySQL Writer跟Oracle Writer;检查PreSQL跟PostSQL语法以及insert，delete权限*/
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查PreSql跟PostSql语句*/
            prePostSqlValid(originalConfig, dataBaseType);
            /*检查insert 跟delete权限*/
            privilegeValid(originalConfig, dataBaseType);
        }

        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查PreSql跟PostSql语句*/
            WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
            WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);
        }

        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查insert 跟delete权限*/
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                boolean hasInsertPri = DBUtil
                    .checkInsertPrivilege(dataBaseType, jdbcUrl, username, password,
                        expandedTables);

                if (!hasInsertPri) {
                    throw RdbmsException
                        .asInsertPriException(dataBaseType, originalConfig.getString(Key.USERNAME),
                            jdbcUrl);
                }

                if (DBUtil.needCheckDeletePrivilege(originalConfig)) {
                    boolean hasDeletePri = DBUtil
                        .checkDeletePrivilege(dataBaseType, jdbcUrl, username, password,
                            expandedTables);
                    if (!hasDeletePri) {
                        throw RdbmsException.asDeletePriException(dataBaseType,
                            originalConfig.getString(Key.USERNAME), jdbcUrl);
                    }
                }
            }
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        public void prepare(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);
                Configuration connConf = Configuration.from(conns.get(0)
                    .toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                originalConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                originalConfig.set(Key.TABLE, table);

                List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                    String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                    preSqls, table);

                originalConfig.remove(Constant.CONN_MARK);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    originalConfig.remove(Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(dataBaseType,
                        jdbcUrl, username, password);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                        StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                originalConfig.toJSON());
        }

        public List<Configuration> split(Configuration originalConfig,
            int mandatoryNumber) {
            return WriterUtil.doSplit(originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                // 已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

                String table = originalConfig.getString(Key.TABLE);

                List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                    String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                    postSqls, table);

                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    // 说明有 postSql 配置，则此处删除掉
                    originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType,
                        jdbcUrl, username, password);

                    LOG.info(
                        "Begin to execute postSqls:[{}]. context info:{}.",
                        StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        public void destroy(Configuration originalConfig) {
        }
    }

    public static class Task {
        protected static final Logger LOG = LoggerFactory
            .getLogger(Task.class);
        private static final String VALUE_HOLDER = "?";
        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;
        protected static String INSERT_OR_REPLACE_TEMPLATE;
        protected DataBaseType dataBaseType;
        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected String conf_pk;
        protected String conf_pkIndex;
        protected List<String> columns;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;
        protected String writeRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;
        private String dataBaseName;

        public Task(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration writerSliceConfig) {
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);

            //ob10的处理
            if (this.jdbcUrl.startsWith(Constant.OB10_SPLIT_STRING) &&
                this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                        .asDataXException(
                            DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() + ":" + this.username;
                this.jdbcUrl = ss[2];
                LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.table = writerSliceConfig.getString(Key.TABLE);
            this.conf_pk = writerSliceConfig.getString(Key.PK, null);
            this.conf_pkIndex = writerSliceConfig.getString(Key.PK_INDEX, null);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize =
                writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            INSERT_OR_REPLACE_TEMPLATE =
                writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                this.jdbcUrl, this.table);
        }

        public void prepare(Configuration writerSliceConfig) {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                this.jdbcUrl, username, password);

            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                this.dataBaseType, BASIC_MESSAGE);

            int tableNumber = writerSliceConfig.getInt(
                Constant.TABLE_NUMBER_MARK);
            if (tableNumber != 1) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                    StringUtils.join(this.preSqls, ";"), BASIC_MESSAGE);
                WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE, dataBaseType);
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver,
            TaskPluginCollector taskPluginCollector, Connection connection) {
            this.taskPluginCollector = taskPluginCollector;

            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                this.table, StringUtils.join(this.columns, ","));
            // 写数据库的SQL语句
            calcWriteRecordSql();

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                long flgTime = System.currentTimeMillis();
                while ((record = recordReceiver.getFromReader()) != null) {

                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        LOG.error("the record is: [{}]", record.toString());
                        throw DataXException
                            .asDataXException(
                                DBUtilErrorCode.CONF_ERROR,
                                String.format(
                                    "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改. Record is [%s]",
                                    record.getColumnNumber(),
                                    this.columnNumber, record.toString()));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize ||
                        System.currentTimeMillis() - flgTime > 10000) {
                        flgTime = System.currentTimeMillis();
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        // TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver,
            Configuration writerSliceConfig,
            TaskPluginCollector taskPluginCollector) {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                this.jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                this.dataBaseType, BASIC_MESSAGE);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
        }

        public void post(Configuration writerSliceConfig) {
            int tableNumber = writerSliceConfig.getInt(
                Constant.TABLE_NUMBER_MARK);

            boolean hasPostSql = (this.postSqls != null && this.postSqls.size() > 0);
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(this.dataBaseType,
                this.jdbcUrl, username, password);

            LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                StringUtils.join(this.postSqls, ";"), BASIC_MESSAGE);
            WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE, dataBaseType);
            DBUtil.closeDBResources(null, null, connection);
        }

        public void destroy(Configuration writerSliceConfig) {
        }

        protected void doBatchInsert(Connection connection, List<Record> buffer)
            throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                    .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(
                        preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        protected void doOneInsert(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            PreparedStatement dps = null;
            try {
                //connection.setAutoCommit(true);
                preparedStatement = connection
                    .prepareStatement(this.writeRecordSql);
                //String sv = "?";
                //if (this.dataBaseType == DataBaseType.PostgreSQL) {
                //    sv = calcValueHolder(this.resultSetMetaData.getRight().get(pkIndex));
                //}
                //String deleteSql = String.format("delete from %s where %s = %s", table, pk, sv);
                //dps = connection.prepareStatement(deleteSql);
                // 当存在主键冲突时，先delete冲突数据
                if (this.dataBaseType == DataBaseType.PostgreSQL || this.dataBaseType == DataBaseType.Oracle) {
                    doDelete(connection, buffer);
                }
                connection.setAutoCommit(true);
                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                        // delete
                        //doDelete(dps, record);
                        // insert
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        LOG.debug(e.toString());
                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    } finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
                //DBUtil.closeDBResources(dps, null);
            }
        }

        private void doDelete(Connection conn, List<Record> records) {
            DatabaseMetaData metaData = null;
            PreparedStatement preparedStatement = null;
            ResultSet primaryKeys = null;
            String sv = "?";
            try {
                metaData = conn.getMetaData();
                //ResultSet catalogs = metaData.getCatalogs();
                //while (catalogs.next()) {
                //    dataBaseName = catalogs.getString(1);
                //}
                primaryKeys = metaData.getPrimaryKeys(null, null, table);
                List<String> pks = new ArrayList<String>();
                List<Integer> pkIndexs = new ArrayList<Integer>();
                List<String> left = this.resultSetMetaData.getLeft();
                while (primaryKeys.next()) {
                    String column_name = primaryKeys.getString("column_name");
                    pks.add(column_name);
                    pkIndexs.add(left.indexOf(column_name));
                }
                if (0 == pks.size() && 0 == pkIndexs.size()) {
                    LOG.info("未正确获取到表:[{}]的主键信息，可能的原因是表名配置或大小写有误，程序将从配置里读取主键字段和下标信息。");
                    if (null != conf_pk && null != conf_pkIndex) {
                        pks = Arrays.asList(conf_pk.split(","));
                        String[] split = conf_pkIndex.split(",");
                        for (String index : split) {
                            pkIndexs.add(Integer.valueOf(index));
                        }
                    }
                }
                LOG.info("Table:[{}] primaryKeys is:{}", table, pks.toString());
                LOG.info("Table:[{}] primaryKeys index is:{}", table, pkIndexs.toString());
                if (0 == pks.size()) {
                    return;
                }
                String deleteSql = "delete from " + table + " where %s = %s";
                for (int i = 0; i < pks.size(); i++) {
                    if (this.dataBaseType == DataBaseType.PostgreSQL) {
                        sv = calcValueHolder(this.resultSetMetaData.getRight().get(pkIndexs.get(i)));
                    }
                    deleteSql = String.format(deleteSql, pks.get(i), sv);
                    if (i == pks.size() - 1) {
                        break;
                    }
                    deleteSql += " and %s = %s";
                }
                //conn.setAutoCommit(false);
                preparedStatement = conn.prepareStatement(deleteSql);
                for (Record record : records) {
                    for (int i = 0; i < pks.size(); i++) {
                        preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, this.resultSetMetaData.getMiddle().get(pkIndexs.get(i)), record.getColumn(pkIndexs.get(i)));
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                LOG.debug(e.toString(), e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    LOG.error(e.toString(), e);
                }
            } finally {
                DBUtil.closeDBResources(primaryKeys, preparedStatement, null);
            }


        }

        private void doDelete(PreparedStatement preparedStatement, Record record) {
            try {
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, 0,
                        this.resultSetMetaData.getMiddle().get(Integer.parseInt(conf_pkIndex)), record.getColumn(Integer.parseInt(conf_pkIndex)));
                preparedStatement.execute();
            } catch (SQLException e) {
                LOG.debug(e.toString());
            } finally {
                try {
                    preparedStatement.clearParameters();
                } catch (SQLException e) {
                    LOG.error(e.toString());
                }
            }
        }

        // 直接使用了两个类变量：columnNumber,resultSetMetaData
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement,
            Record record)
            throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                preparedStatement =
                    fillPreparedStatementColumnType(preparedStatement, i, columnSqltype,
                        record.getColumn(i));
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(
            PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column)
            throws SQLException {
            java.util.Date utilDate;

            //String rawData = column.asString();
            //if (StringUtils.isNotEmpty(rawData)) {
            //    preparedStatement.setString(columnIndex + 1, rawData);
            //} else {
            //    preparedStatement.setString(columnIndex + 1, null);
            //}

            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    preparedStatement.setString(columnIndex + 1, column
                        .asString());
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && StringUtils.isEmpty(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    //Long longValue = column.asLong();
                    String longValue = column.asString();
                    if (StringUtils.isEmpty(longValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, longValue);
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (this.resultSetMetaData.getRight().get(columnIndex)
                        .equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement
                                .setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        } catch (DataXException e) {
                            utilDate = null;
                            LOG.warn("Date 类型转换错误：[{}], 该值置为null", column);
                            //throw new SQLException(String.format(
                            //        "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        utilDate = null;
                        LOG.warn("Date 类型转换错误：[{}], 该值置为null", column);
                        //throw new SQLException(String.format(
                        //        "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        utilDate = null;
                        LOG.warn("Date 类型转换错误：[{}], 该值置为null", column);
                        //throw new SQLException(String.format(
                        //        "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                            utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1,
                        StringUtils.isEmpty(column.asString()) ? null : column
                            .asBytes());
                    break;

                case Types.BOOLEAN:
                    preparedStatement.setString(columnIndex + 1,
                        StringUtils.isEmpty(column.asString()) ? null : column.asString());
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    } else {
                        preparedStatement.setString(columnIndex + 1, column.asString());
                    }
                    break;
                default:
                    throw DataXException
                        .asDataXException(
                            DBUtilErrorCode.UNSUPPORTED_TYPE,
                            String.format(
                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                this.resultSetMetaData.getLeft()
                                    .get(columnIndex),
                                this.resultSetMetaData.getMiddle()
                                    .get(columnIndex),
                                this.resultSetMetaData.getRight()
                                    .get(columnIndex)));
            }
            return preparedStatement;
        }

        private void calcWriteRecordSql() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                boolean forceUseUpdate = false;
                //ob10的处理
                if (dataBaseType != null && dataBaseType == DataBaseType.MySql &&
                    OriginalConfPretreatmentUtil.isOB10(jdbcUrl)) {
                    forceUseUpdate = true;
                }

                INSERT_OR_REPLACE_TEMPLATE = WriterUtil
                    .getWriteTemplate(columns, valueHolders, writeMode, dataBaseType,
                        forceUseUpdate);
                writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
            }
        }

        protected String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }
    }
}
