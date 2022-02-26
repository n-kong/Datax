package com.alibaba.datax.plugin.writer.datahubwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.writer.datahubwriter.qax.TipDatahubClient;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private String projectName;
        private String topicName;
        private String accountType;
        private DatahubClient client;
        private String runEnv;

        @Override public void init() {
            LOG.info("datahubwriter job start init()");
            this.originalConfig = super.getPluginJobConf();
            DatahubUtil.checkNecessaryConfig(this.originalConfig);
            //
            // DatahubUtil.dealMaxRetryTime(this.originalConfig);
            this.runEnv = this.originalConfig.getString(Key.RUN_ENV, "");
            this.projectName = this.originalConfig.getString(Key.PROJECT);
            this.topicName = this.originalConfig.getString(Key.TOPIC);
            this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);
            if (!Constant.DEFAULT_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType) &&
                !Constant.TAOBAO_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType)) {
                throw DataXException.asDataXException(DatahubWriterErrorCode.ACCOUNT_TYPE_ERROR,
                    String.format(
                        "账号类型错误，因为你的账号 [%s] 不是datax目前支持的账号类型，目前仅支持aliyun, taobao账号，请修改您的账号信息.",
                        accountType));
            }
            this.originalConfig.set(Key.ACCOUNT_TYPE, this.accountType);
            if (IS_DEBUG) {
                LOG.debug("After master init(), job config now is: [\n{}\n] .",
                    this.originalConfig.toJSON());
            }
            LOG.info("datahubwriter job end init()");
        }

        @Override public void prepare() {
            if ("GA".equalsIgnoreCase(runEnv)) {
                return;
            }
            this.client = DatahubUtil.initDatahubClient(this.originalConfig);
            boolean isExist =
                client.listTopic(this.projectName).getTopics().contains(this.topicName);
            if (!isExist) {
                LOG.warn("Project:{}, Topic:{} is not exist!", this.projectName, this.topicName);
                createTopic();
            }
        }

        public void createTopic() {
            List<String> schemas = originalConfig.getList(Key.SCHEMA, String.class);
            if (null != schemas) {
                if (1 == schemas.size() && "*".equals(schemas.get(0))) {
                    throw DataXException.asDataXException(DatahubWriterErrorCode.ILLEGAL_VALUE,
                        String
                            .format("Topic:[%S] 不存在, 并且您未配置正确的Schema信息, 请重新配置! ", this.topicName));
                }
                RecordSchema recordSchema = new RecordSchema();
                for (String schema : schemas) {
                    recordSchema.addField(new Field(schema, FieldType.STRING));
                }
                this.client.createTopic(this.projectName, this.topicName, 1, 7, RecordType.TUPLE,
                    recordSchema, this.topicName);
                this.client.waitForShardReady(this.projectName, this.topicName);
            }
        }

        @Override public List<Configuration> split(int mandatoryNumber) {

            List<Configuration> configurations = new ArrayList<Configuration>();

            RecordSchema recordSchema = null;

            if ("GA".equalsIgnoreCase(runEnv)) {
                recordSchema = TipDatahubClient.instance().getTopicSchema(projectName, topicName);
            } else {
                recordSchema =
                    this.client.getTopic(this.projectName, this.topicName).getRecordSchema();
            }

            List<String> allSchemas = DatahubUtil.getAllSchema(recordSchema);

            LOG.info("allSchemaList: {} .", StringUtils.join(allSchemas, ','));

            dealColumn(this.originalConfig, allSchemas);

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tempConfig = this.originalConfig.clone();
                configurations.add(tempConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("After master split, the job config now is:[\n{}\n].",
                    this.originalConfig);
            }

            return configurations;
        }

        private void dealColumn(Configuration originalConfig, List<String> allColumns) {
            //之前已经检查了userConfiguredColumns 一定不为空
            List<String> userConfiguredColumns = originalConfig.getList(Key.SCHEMA, String.class);
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                userConfiguredColumns = allColumns;
                originalConfig.set(Key.SCHEMA, allColumns);
            } else {
                //检查列是否重复，大小写不敏感（所有写入，都是不允许写入段的列重复的）
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                //检查列是否存在，大小写不敏感
                ListUtil.makeSureBInA(allColumns, userConfiguredColumns, false);
            }

            List<Integer> columnPositions =
                DatahubUtil.parsePosition(allColumns, userConfiguredColumns);
            originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
        }

        @Override public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration sliceConfig;
        private DatahubClient client = null;

        private String projectName;
        private String topicName;
        private int batchSize;
        private String runEnv;
        private RecordSchema recordSchema;

        @Override public void init() {

            this.sliceConfig = super.getPluginJobConf();

            this.projectName = this.sliceConfig.getString(Key.PROJECT);
            this.topicName = this.sliceConfig.getString(Key.TOPIC);
            this.batchSize = Integer.parseInt(this.sliceConfig.getString(Key.BATCH_SIZE));
            this.runEnv = this.sliceConfig.getString(Key.RUN_ENV, "");
            if (IS_DEBUG) {
                LOG.debug("After init in task, sliceConfig now is:[\n{}\n].", this.sliceConfig);
            }
        }

        @Override public void prepare() {

            if ("GA".equalsIgnoreCase(runEnv)) {
                recordSchema = TipDatahubClient.instance().getTopicSchema(projectName, topicName);
            } else {
                this.client = DatahubUtil.initDatahubClient(this.sliceConfig);
                recordSchema = this.client.getTopic(this.projectName, this.topicName).getRecordSchema();

            }
        }

        @Override public void startWrite(RecordReceiver recordReceiver) {

            List<Integer> columnPositions = this.sliceConfig.getList(Constant.COLUMN_POSITION,
                Integer.class);

            try {
                TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();

                DatahubWriterProxy proxy =
                    new DatahubWriterProxy(columnPositions, taskPluginCollector,
                        projectName, topicName, batchSize, recordSchema, sliceConfig);
                if (null != client) {
                    proxy.setDatahubClient(client);
                }
                proxy.setRunEnv(runEnv);
                com.alibaba.datax.common.element.Record dataXRecord = null;

                while ((dataXRecord = recordReceiver.getFromReader()) != null) {
                    try {
                        proxy.writeRecord(dataXRecord);
                    } catch (Exception e) {
                        LOG.error("write datahub topic failed! Error Msg:[{}]", e.getMessage(),e);
                        this.client.close();
                        this.client = DatahubUtil.initDatahubClient(sliceConfig);
                        proxy.setDatahubClient(client);
                    }
                }

                proxy.writeRemainingRecord();
            } catch (Exception e) {
                throw DataXException.asDataXException(DatahubWriterErrorCode.WRITER_RECORD_FAIL,
                    "写入 Datahub 目的Topic失败.", e);
            } finally {
                if (null != client) {
                    client.close();
                }
            }
        }

        @Override public void destroy() {

        }
    }
}
