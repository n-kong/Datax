package com.alibaba.datax.plugin.writer.bkwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.bkwriter.http.HttpService;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/2/2
 */
public class BkWriterProxy {

    private static Logger LOG = LoggerFactory.getLogger(BkWriterProxy.class);
    private String url;
    private int objectIdIndex;
    private int imageIndex;
    private String bkId;
    private int batchSize;
    private List<Map> lists;
    private String imageType;
    private Map<String, Object> map = new HashMap<String, Object>();
    private Map<String, Object> data = new HashMap<String, Object>();

    public BkWriterProxy(String url, int objectIdIndex, int imageIndex, String bkId, int batchSize, String imageType) {
        this.url = url;
        this.objectIdIndex = objectIdIndex;
        this.imageIndex = imageIndex;
        this.bkId = bkId;
        this.batchSize = batchSize;
        this.lists = new ArrayList<Map>();
        this.imageType = imageType;
        this.initMap();
    }

    private void initMap() {
        this.data.put("objects", lists);
        this.data.put("bkid", bkId);
        this.map.put("data", this.data);
    }

    public void writeRecord(Record record) {

        int columnNumber = record.getColumnNumber();
        if (0 == columnNumber) {
            return;
        }
        Map<String, String> map = new HashMap<String, String>();
        map.put("objectId", record.getColumn(objectIdIndex).asString());
        map.put(imageType, record.getColumn(imageIndex).asString());
        lists.add(map);
        if (lists.size() >= batchSize) {
            write();
            LOG.info("write to bkService success num {}.", lists.size());
            lists.clear();
        }
    }

    public void writeRemainingRecord() {
        if (lists.size() != 0) {
            write();
            LOG.info("write to bkService success num {}.", lists.size());
            lists.clear();
        }
    }

    private void write() {
        //String entityStr = buildRequestParams();
        HttpService.sendPost(url, new HashMap<String, String>(), JSONObject.toJSONString(this.map));
    }

    //private String buildRequestParams() {
    //    JSONObject params = new JSONObject();
    //    params.put("objects", lists);
    //    params.put("bkid", bkId);
    //
    //    JSONObject json = new JSONObject();
    //    json.put("data", params);
    //
    //    return json.toJSONString();
    //}

}
