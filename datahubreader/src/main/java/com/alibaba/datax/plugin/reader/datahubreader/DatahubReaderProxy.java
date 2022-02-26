package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubReaderProxy {

    private static final Logger LOG = LoggerFactory.getLogger(DatahubReaderProxy.class);

    private Thread thread = null;
    private String projectName;
    private String topicName;
    private String subId;
    private String shardId;
    private RecordSender recordSender;
    private Integer recordNum;
    private GetCursorRequest.CursorType readType;
    private Configuration conf;
    private DatahubClient client;

    public DatahubReaderProxy(Configuration conf, RecordSender recordSender,
        GetCursorRequest.CursorType readType) {
        this.readType = readType;
        this.recordSender = recordSender;
        this.conf = conf;
        this.projectName = this.conf.getString(Key.PROJECT);
        this.topicName = this.conf.getString(Key.TOPIC);
        this.subId = this.conf.getString(Key.SUBID);
        this.recordNum = this.conf.getInt(Key.RECORDNUM, 100);
        this.shardId = this.conf.getString(Key.SHARDID);
    }

    public void read() {
        client = DatahubReaderUtils.getInstance().initDatahubClient(conf);
        RecordSchema schema = client.getTopic(projectName, topicName).getRecordSchema();
        OffsetContext offset = client.initOffsetContext(projectName, topicName, subId, shardId);
        String cursor = getCursor(offset, shardId, readType);
        boolean isExist = true;
        while (isExist) {
            ShardState shardState = client.listShard(projectName, topicName).getShards()
                .get(Integer.valueOf(shardId)).getState();

            if ("ACTIVE".equalsIgnoreCase(shardState.toString())) {
                try {
                    GetRecordsResult recordRs = client.getRecords(
                        projectName, topicName, shardId, cursor, recordNum, schema);
                    List<RecordEntry> recordEntries = recordRs.getRecords();
                    if (recordEntries.size() == 0) {
                        try {
                            LOG.info(
                                "subid = {}, read datahub {}.{} shard = {}. no data now,sleep 30s.",
                                subId, projectName, topicName, shardId);
                            recordSender.flush();
                            Thread.sleep(30000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        for (RecordEntry recordEntry : recordEntries) {
                            offset.setOffset(recordEntry.getOffset());
                            recordSender
                                .sendToWriter(buildRecord(recordSender, recordEntry));
                        }

                        client.commitOffset(offset);
                    }
                    // 拿到下一个游标
                    cursor = recordRs.getNextCursor();
                } catch (InvalidCursorException ex) {
                    cursor = getCursor(offset, shardId, readType);
                    LOG.error("InvalidCursorException {} , and get new cursor : {}",
                        ex.getMessage(), cursor, ex);
                } catch (DatahubClientException ex) {
                    LOG.error("DatahubClientException {}", ex.getMessage(), ex);
                    isExist = false;
                }
            }
        }
    }

    private String getCursor(OffsetContext offsetCtx, String shardId,
        GetCursorRequest.CursorType readType) {
        String cursor;
        GetCursorResult cursorRs = null;
        if (!offsetCtx.hasOffset()) {
            cursorRs = client.getCursor(projectName, topicName, shardId, readType);
            cursor = cursorRs.getCursor();
        } else {
            if (offsetCtx.getOffset().getTimestamp() <
                client.getCursor(projectName, topicName, shardId, readType).getRecordTime()) {
                // 如果消费的较慢，当前的offset已经小于数据的最老时间。则需要判断是否还要消费。
                cursorRs = client.getCursor(projectName, topicName, shardId, readType);
                cursor = cursorRs.getCursor();
                LOG.info("old consume offset is = {}", offsetCtx.getOffset().getTimestamp());
                LOG.info("new consume offset is = {}, shard ={}",
                    client.getCursor(projectName, topicName, shardId, readType).getRecordTime(),
                    shardId);
            } else {
                cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
                LOG.info("new circulation shard ={}, offset = {}", shardId,
                    offsetCtx.getOffset().getTimestamp());
            }
        }
        return cursor;
    }

    private Record buildRecord(RecordSender recordSender, RecordEntry recordEntry) {
        Record record = recordSender.createRecord();
        int count = recordEntry.getFieldCount();
        for (int i = 0; i < count; i++) {
            record.addColumn(new StringColumn(recordEntry.getString(i)));
        }
        return record;
    }
}
