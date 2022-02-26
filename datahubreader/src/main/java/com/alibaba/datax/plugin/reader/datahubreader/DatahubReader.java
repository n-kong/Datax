package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidCursorException;
import com.aliyun.datahub.model.GetCursorRequest;
import com.aliyun.datahub.model.GetCursorResult;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.ShardState;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;
        private String projectName;
        private String topicName;
        private String accountType;
        private DatahubClient client = null;

        @Override public void init() {

            LOG.info("datahubreader job start init()");
            this.originalConfig = super.getPluginJobConf();
            DatahubReaderUtils.checkNecessaryConfig(this.originalConfig);

            this.projectName = this.originalConfig.getString(Key.PROJECT);
            this.topicName = this.originalConfig.getString(Key.TOPIC);
            this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);
            if (!Constant.DEFAULT_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType) &&
                !Constant.TAOBAO_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType)) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.ACCOUNT_TYPE_ERROR,
                    String.format(
                        "账号类型错误，因为你的账号 [%s] 不是datax目前支持的账号类型，目前仅支持aliyun, taobao账号，请修改您的账号信息.",
                        accountType));
            }
            this.originalConfig.set(Key.ACCOUNT_TYPE, this.accountType);

            LOG.info("datahubreader job end init()");
        }

        @Override public void prepare() {
            LOG.info("datahubreader Job start prepare()");
            this.client = DatahubReaderUtils.getInstance().initDatahubClient(this.originalConfig);
            boolean isExist =
                client.listTopic(this.projectName).getTopics().contains(this.topicName);

            if (!isExist) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.TOPIC_VALUE,
                    String.format("Project:[%s], Topic:[%s]不存在，请检查配置！", projectName, topicName));
            }
            LOG.info("datahubreader Job start prepare()");
        }

        @Override public List<Configuration> split(int mandatoryNumber) {
            LOG.info("datahubreader Job start split()");
            List<Configuration> configurations = new ArrayList<Configuration>();
            int shardCount = client.getTopic(projectName, topicName).getShardCount();
            for (int i = 0; i < shardCount; i++) {
                Configuration tempConfig = this.originalConfig.clone();
                tempConfig.set(Key.SHARDID, i);
                configurations.add(tempConfig);
            }
            LOG.info("datahubreader Job end split()");
            this.client.close();
            return configurations;
        }

        @Override public void post() {

        }

        @Override public void destroy() {

        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration sliceConfig;
        private DatahubClient client = null;

        private String projectName;
        private String topicName;
        private String subId;
        private GetCursorRequest.CursorType readType;
        private Integer recordNum;
        private String shardId;

        @Override public void init() {

            this.sliceConfig = super.getPluginJobConf();

            this.projectName = this.sliceConfig.getString(Key.PROJECT);
            this.topicName = this.sliceConfig.getString(Key.TOPIC);
            this.subId = this.sliceConfig.getString(Key.SUBID);
            formatType(this.sliceConfig.getString(Key.READTYPE, "OLDEST"));
            this.recordNum = this.sliceConfig.getInt(Key.RECORDNUM, 100);
            this.shardId = this.sliceConfig.getString(Key.SHARDID);

            if (IS_DEBUG) {
                LOG.debug("After init in task, sliceConfig now is:[\n{}\n].", this.sliceConfig);
            }
        }



        private void formatType(String type) {
            switch (type.toUpperCase()) {
                case "OLDEST":
                    this.readType = GetCursorRequest.CursorType.OLDEST;
                    break;
                case "LATEST":
                    this.readType = GetCursorRequest.CursorType.LATEST;
                    break;
                case "SYSTEM_TIME":
                    this.readType = GetCursorRequest.CursorType.SYSTEM_TIME;
                    break;
                default:
                    LOG.warn("您未配置消费模式, 默认采用OLDEST模式消费数据!");
                    this.readType = GetCursorRequest.CursorType.OLDEST;
            }
        }

        @Override public void prepare() {

        }

        @Override public void startRead(RecordSender recordSender) {
            //if (null == this.client) {
            //    this.client = DatahubReaderUtils.getInstance().initDatahubClient(this.sliceConfig);
            //}

            DatahubReaderProxy datahubReaderProxy = new DatahubReaderProxy(this.sliceConfig, recordSender, readType);
            datahubReaderProxy.read();


        }

        @Override public void post() {

        }

        @Override public void destroy() {
            if (null != client) {
                this.client.close();
            }
        }
    }
}
