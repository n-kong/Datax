package com.alibaba.datax.plugin.writer.datahubwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.datahubwriter.qax.TipDatahubClient;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubWriterProxy {

    private static final Logger LOG = LoggerFactory
        .getLogger(DatahubWriterProxy.class);

    private volatile boolean printColumnLess;
    private DatahubClient client;
    private List<Integer> columnPositions;
    private TaskPluginCollector taskPluginCollector;
    private String project;
    private String topic;
    private RecordSchema recordSchema;
    private List<RecordEntry> recordList;
    private int batchSize;
    private String runEnv;
    private Configuration config;
    private long time;

    public DatahubWriterProxy(List<Integer> columnPositions,
        TaskPluginCollector taskPluginCollector, String project, String topic, int batchSize, RecordSchema recordSchema, Configuration config) {
        this.columnPositions = columnPositions;
        this.taskPluginCollector = taskPluginCollector;
        this.project = project;
        this.topic = topic;
        this.recordSchema = recordSchema;
        this.config = config;
        this.recordList = new ArrayList<>();
        this.batchSize = batchSize;
        time = System.currentTimeMillis();
        printColumnLess = true;
    }

    public void setDatahubClient(DatahubClient client) {
        this.client = client;
    }
    public void setRunEnv(String env) {
        this.runEnv = env;
    }

    public void writeRecord(Record dataXRecord) {
        RecordEntry record = dataxRecordToDatahubRecord(dataXRecord);
        if (null != record) {
            recordList.add(record);
            if (recordList.size() >= batchSize) {
                write();
            }
        }
    }

    public void writeRemainingRecord() {
        if (recordList.size() != 0) {
            write();
        }
    }

    private void write() {
        PutRecordsResult result = null;
        try {
            if ("GA".equalsIgnoreCase(runEnv)) {
                TipDatahubClient.instance().putRecords(project, topic, recordList);
                LOG.info("write to {}.{} success num = {}", project, topic, recordList.size());
            } else {
                if(null == this.client) {
                    LOG.info("datahubClient is null, init!");
                    this.client = DatahubUtil.initDatahubClient(config);
                }
                try {
                    result = this.client.putRecords(project, topic, recordList);
                } catch (Exception e) {
                    LOG.error("client error:{}", e);
                    LOG.error("result:{}", result);
                }
                if (result != null && result.getFailedRecordCount() == 0) {
                    LOG.info("write to {}.{} success num = {}", project, topic, recordList.size());
                } else {
                    LOG.error("result:{}", result);
                    //LOG.error("write to {}.{} failed num = {}, ErrorCode: {}, Message : {}", project,
                    //    topic, result.getFailedRecordCount(),
                    //    result.getFailedRecordError().get(0).getErrorcode(),
                    //    result.getFailedRecordError().get(0).getMessage());
                }
            }
            recordList.clear();
        } catch (Exception e) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            if (null != this.client) {
                this.client.close();
            }
            LOG.error("Init DatahubClient");
            this.client = DatahubUtil.initDatahubClient(config);
            LOG.error("写入 Datahub 失败: 失败记录为: {}", result.getFailedRecords().toString(), e);
        }
    }

    private RecordEntry dataxRecordToDatahubRecord(Record dataXRecord) {

        int sourceColumnCount = dataXRecord.getColumnNumber();

        RecordEntry recordEntry = new RecordEntry(recordSchema);

        int userConfiguredColumnNumber = this.columnPositions.size();

        if (sourceColumnCount > userConfiguredColumnNumber) {
            throw DataXException
                .asDataXException(
                    DatahubWriterErrorCode.ILLEGAL_VALUE,
                    String.format(
                        "配置中的源Topic的列个数和目的端Topic不一致，源Topic中您配置的列数是:%s 大于目的端的列数是:%s , 这样会导致源头数据无法正确导入目的端, 请检查您的配置并修改.",
                        sourceColumnCount,
                        userConfiguredColumnNumber));
        } else if (sourceColumnCount < userConfiguredColumnNumber) {
            if (printColumnLess) {
                LOG.warn(
                    "源Topic的列个数小于目的Topic的列个数，源Topic列数是:{} 目的Topic列数是:{} , 数目不匹配. DataX 会把目的端多出的列的值设置为空值. 如果这个默认配置不符合您的期望，请保持源Topic和目的Topic配置的列数目保持一致.",
                    sourceColumnCount, userConfiguredColumnNumber);
            }
            printColumnLess = false;
        }

        int currentIndex;
        int sourceIndex = 0;
        try {
            com.alibaba.datax.common.element.Column columnValue;

            for (; sourceIndex < sourceColumnCount; sourceIndex++) {
                currentIndex = columnPositions.get(sourceIndex);
                columnValue = dataXRecord.getColumn(sourceIndex);

                if (columnValue == null) {
                    continue;
                }

                recordEntry.setString(currentIndex, columnValue.asString());
            }

            return recordEntry;
        } catch (Exception e) {
            String message = String.format(
                "写入 Datahub 时遇到了脏数据: 第[%s]个字段的数据出现错误，请检查该数据并作出修改 或者您可以增大阀值，忽略这条记录.", sourceIndex);
            this.taskPluginCollector.collectDirtyRecord(dataXRecord, e,
                message);

            return null;
        }
    }
}
