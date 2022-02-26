package com.alibaba.datax.plugin.writer.datahubwriter.qax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TipDatahubClient
 *
 * @author rh
 * @date 2020-07-09 10:44
 */
public class TipDatahubClient {

    private final static Logger LOGGER = LoggerFactory.getLogger(TipDatahubClient.class);
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String endport;
    private static TipDatahubClient tipClient;
    private static QianxinClientV2 qianxinClientV2;
    static {
        qianxinClientV2 = new QianxinClientV2("3b0f47","3ccd8040863b6390","https://41.196.13.10:6443/sts/token","qbsj.sjjhpt.hzs.zj");
        tipClient = new TipDatahubClient();
        accessKeyId = "3dVcAZIqyKqnwaNV";
        accessKeySecret = "DzJOutCxvk2gWC2s9jKr1lZksTeO7J";
        endport = "http://139.36.1.160";
    }

	public static TipDatahubClient instance() {
        return tipClient;
    }

    private static Map<String,List<String>> fieldsCache = new HashMap<>();
    private static Map<String,String> fieldsTypeCache = new HashMap<>();

    public void putRecords(String project, String topic, List<RecordEntry> records){
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", accessKeyId);
        jsonObject.put("passwd", accessKeySecret);
        jsonObject.put("endPoint", endport);
        jsonObject.put("projectName", project);
        jsonObject.put("topicName", topic);
        List<String> fieldsList = fieldsCache.get(project + "." + topic);
        String fieldsType = fieldsTypeCache.get(project + "." + topic);
        if(null == fieldsList || null == fieldsType){
        	fieldsList = new ArrayList<String>();
        	StringBuffer sb = new StringBuffer();
            List<Field> fields = getTopicSchema(project, topic).getFields();
            Iterator<Field> fieldsIterator = fields.iterator();
            while(fieldsIterator.hasNext()){
            	Field f = fieldsIterator.next();
            	String name = f.getName();
            	fieldsList.add(name);
            	sb.append(f.getType().name()).append("::").append(name);
            	if(fieldsIterator.hasNext()){
            		sb.append(",");
            	}
            }
            fieldsType = sb.toString();
            if(fieldsList.isEmpty()|| StringUtils.isBlank(fieldsType)){
                LOGGER.error("获得topic schema异常！获得的schame字段为空！");
                return;
            }
            fieldsCache.put(project + "." + topic,fieldsList);
            fieldsTypeCache.put(project + "." + topic,fieldsType);
        }
        jsonObject.put("fields", fieldsType);

        List<Object> recordsList = new ArrayList<>();
        for(RecordEntry re:records){

            List<String> record = new ArrayList<>();

            Field[] fields = re.getFields();

			for (Field field : fields) {
				String name = field.getName();
                String value = re.getString(name);
                record.add(null == value? "":value);
            }
            recordsList.add(record);

            //Map<String, String> attributes = re.getAttributes();
            //for(String s:fieldsList){
            //	String fv = attributes.get(s);
            //    record.add(null==fv?"":fv);
            //}
            //recordsList.add(record);
        }
        final JSONArray jsonArray = new JSONArray(recordsList);
        jsonObject.put("records", jsonArray);
        String jsonString = jsonObject.toJSONString();
        HashMap head = new HashMap();
        head.put("Content-Type", "application/json; charset=UTF-8");
        qianxinClientV2.sendPost("https://41.196.13.10:6443/upload/datahub",jsonString,head);
    }


    public RecordSchema getTopicSchema(String project, String topic){
        RecordSchema schema = new RecordSchema();
        String url = "https://41.196.13.10:6443/upload/getTopicSchema";
        Map<String,String> head = new HashMap<>();
        head.put("name", accessKeyId);
        head.put("passwd", accessKeySecret);
        head.put("endPoint",endport);
        head.put("projectName", project);
        head.put("topicName", topic);

        String reponse = qianxinClientV2.sendGet(url,head);
        JSONObject reponseObject = JSONObject.parseObject(reponse);
        String code = String.valueOf(reponseObject.get("code"));
        if(!"200".equalsIgnoreCase(code)){
            String msg = String.valueOf(reponseObject.get("msg"));
            LOGGER.error("获得topic:{} schema失败！msg:{}", topic, msg);
        }else {
            JSONObject data = JSONObject.parseObject(reponseObject.getString("data"));
            JSONArray fields = data.getJSONArray("fields");
            Iterator<Object> iterator = fields.iterator();
            while (iterator.hasNext()){
                JSONObject field= (JSONObject)iterator.next();
                String name = field.getString("name");
                FieldType fieldType = FieldType.valueOf(field.getString("type"));
                schema.addField(new Field(name, fieldType));
            }
            //LOGGER.info("获得topic schema成功！schema:{}", schema.toJsonString());
			LOGGER.info("获得{}.{} schema成功！", project, topic);
        }
        return schema;
    }

    //public static void main(String[] args) {
    //    ArrayList<RecordEntry> list = new ArrayList<>();
    //    for(int i=0;i<5;i++){
    //    	RecordEntry entry = new RecordEntry();
    //    	entry.addAttribute("ogg_time", "rhtest"+i);
    //    	list.add(entry);
    //    }
    //    TipDatahubClient.putRecords("hzow_kx","ogghub_v3",list);
    //
    //}

}
