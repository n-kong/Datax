package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.JdbcConnectionFactory;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

		private static final Logger logger = LoggerFactory.getLogger(Job.class);
		private static final double V = 9.5;
		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE, "insert").trim().toLowerCase();

			String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
				Constant.CONN_MARK, Key.JDBC_URL));
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);
			String table = originalConfig.getList(String.format("%s[0].%s",
				Constant.CONN_MARK, Key.TABLE), String.class).get(0);
			Connection conn = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password).getConnecttion();
			try {
				double version = Double.valueOf(conn.getMetaData().getDatabaseProductVersion());
				if (version <= V) {
					logger.warn("您的数据库版本为：{}，该版本不支持update，若遇到主键冲突情况，将会以delete-insert模式更新数据。", version);
					writeMode = "insert";
				}

				if (writeMode.startsWith("update")) {
					DatabaseMetaData metaData = conn.getMetaData();
					ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, table);
					List<String> pks = new ArrayList<String>();
					while (primaryKeys.next()) {
						String column_name = primaryKeys.getString("column_name");
						pks.add(column_name);
					}
					if (pks.size() != 0) {
						writeMode = "update(" + StringUtils.join(pks, ",") + ")";
					} else {
						throw DataXException.asDataXException(PostgresqlWriterErrorCode.REQUIRED_VALUE, "未正确识别到表的主键信息，请确认该表是否包含主键，若无主键，请使用insert模式，请您检查配置或在writeMode参数显示配置表主键，例如：writeMode='update(id1,id2)'");
					}

				}
			} catch (SQLException e) {
				logger.error("获取连接失败。 msg:{}", e.getMessage(), e);
			} finally {
				DBUtil.closeDBResources(null, null, conn);
			}
			this.originalConfig.set(Key.WRITE_MODE, writeMode);

			this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			this.commonRdbmsWriterMaster.init(this.originalConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterMaster.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterMaster.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterMaster.destroy(this.originalConfig);
		}

	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE){
				@Override
				public String calcValueHolder(String columnType){
					if("serial".equalsIgnoreCase(columnType)){
						return "?::int";
					}else if("bit".equalsIgnoreCase(columnType)){
						return "?::bit varying";
					}
					return "?::" + columnType;
				}
			};
			this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
		}

		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
		}

	}

}
