package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // warn: 忽略大小写

            String topic = this.originalConfig
                    .getString(Key.TOPIC);
            Integer partitions = this.originalConfig
                    .getInt(Key.KAFKA_PARTITIONS);
            String bootstrapServers = this.originalConfig
                    .getString(Key.BOOTSTRAP_SERVERS);

            String groupId = this.originalConfig
                    .getString(Key.GROUP_ID);
            Integer columnCount = this.originalConfig
                    .getInt(Key.COLUMNCOUNT);
            String split = this.originalConfig.getString(Key.SPLIT);
            String filterContaintsStr = this.originalConfig.getString(Key.CONTAINTS_STR);
            String filterContaintsFlag = this.originalConfig.getString(Key.CONTAINTS_STR_FLAG);
            String conditionAllOrOne = this.originalConfig.getString(Key.CONDITION_ALL_OR_ONE);
            String parsingRules = this.originalConfig.getString(Key.PARSING_RULES);
            String writerOrder = this.originalConfig.getString(Key.WRITER_ORDER);
            String kafkaReaderColumnKey = this.originalConfig.getString(Key.KAFKA_READER_COLUMN_KEY);

            if (null == topic) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.TOPIC_ERROR,
                        "没有设置参数[topic].");
            }
            if (partitions == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARTITION_ERROR,
                        "没有设置参数[kafka.partitions].");
            } else if (partitions < 1) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARTITION_ERROR,
                        "[kafka.partitions]不能小于1.");
            }
            if (null == bootstrapServers) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.ADDRESS_ERROR,
                        "没有设置参数[bootstrap.servers].");
            }
            if (null == groupId) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置参数[groupid].");
            }

            if (columnCount == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARTITION_ERROR,
                        "没有设置参数[columnCount].");
            } else if (columnCount < 1) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "[columnCount]不能小于1.");
            }
            if (null == split) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "[split]不能为空.");
            }
            if (filterContaintsStr != null) {
                if (conditionAllOrOne == null || filterContaintsFlag == null) {
                    throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                            "设置了[filterContaintsStr],但是没有设置[conditionAllOrOne]或者[filterContaintsFlag]");
                }
            }
            if (parsingRules == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[parsingRules]参数");
            } else if (!parsingRules.equals("regex") && parsingRules.equals("json") && parsingRules.equals("split")) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "[parsingRules]参数设置错误，不是regex，json，split其中一个");
            }
            if (writerOrder == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[writerOrder]参数");
            }
            if (kafkaReaderColumnKey == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[kafkaReaderColumnKey]参数");
            }
        }

        @Override
        public void preCheck() {
            init();

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            Integer partitions = this.originalConfig.getInt(Key.KAFKA_PARTITIONS);
            for (int i = 0; i < partitions; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory
                .getLogger(CommonRdbmsReader.Task.class);
        //配置文件
        private Configuration readerSliceConfig;
        //kafka消息的分隔符
        private String split;
        //解析规则
        private String parsingRules;
        //是否停止拉去数据
        private boolean flag;
        //kafka address
        private String bootstrapServers;
        //kafka groupid
        private String groupId;
        //kafkatopic
        private String kafkaTopic;
        //kafka中的数据一共有多少个字段
        private int count;
        //是否需要data_from
        //kafka ip 端口+ topic
        //将包含/不包含该字符串的数据过滤掉
        private String filterContaintsStr;
        //是包含containtsStr 还是不包含
        //1 表示包含 0 表示不包含
        private int filterContaintsStrFlag;
        //全部包含或不包含，包含其中一个或者不包含其中一个。
        private int conditionAllOrOne;
        //writer端要求的顺序。
        private String writerOrder;
        //kafkareader端的每个关键子的key
        private String kafkaReaderColumnKey;
        //异常文件路径
        private String exceptionPath;
        // 消费策略
        private String offsetMode;

        @Override
        public void init() {
            flag = true;
            this.readerSliceConfig = super.getPluginJobConf();
            split = this.readerSliceConfig.getString(Key.SPLIT);
            bootstrapServers = this.readerSliceConfig.getString(Key.BOOTSTRAP_SERVERS);
            groupId = this.readerSliceConfig.getString(Key.GROUP_ID);
            kafkaTopic = this.readerSliceConfig.getString(Key.TOPIC);
            count = this.readerSliceConfig.getInt(Key.COLUMNCOUNT);
            filterContaintsStr = this.readerSliceConfig.getString(Key.CONTAINTS_STR);
            filterContaintsStrFlag = this.readerSliceConfig.getInt(Key.CONTAINTS_STR_FLAG);
            conditionAllOrOne = this.readerSliceConfig.getInt(Key.CONTAINTS_STR_FLAG);
            parsingRules = this.readerSliceConfig.getString(Key.PARSING_RULES);
            writerOrder = this.readerSliceConfig.getString(Key.WRITER_ORDER);
            kafkaReaderColumnKey = this.readerSliceConfig.getString(Key.KAFKA_READER_COLUMN_KEY);
            exceptionPath = this.readerSliceConfig.getString(Key.EXECPTION_PATH);
            offsetMode = this.readerSliceConfig.getString(Key.OFFSET_MODE);
            LOG.info(filterContaintsStr);
        }

        @Override
        public void startRead(RecordSender recordSender) {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null ? groupId : UUID.randomUUID().toString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetMode);
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 50000);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Collections.singletonList(kafkaTopic));
            Record oneRecord = null;
            while (flag) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    String value = record.value();

                    if (StringUtils.isEmpty(value)) {
                        continue;
                    }
                    //定义过滤标志
                    int ifNotContinue = filterMessage(value);
                    //如果标志修改为1了那么就过滤掉这条数据。
                    if (ifNotContinue == 1) {
                        LOG.info("过滤数据： " + record.value());
                        continue;
                    }
                    oneRecord = buildOneRecord(recordSender, value);
                    //如果返回值不等于null表示不是异常消息。
                    if (oneRecord != null) {
                        recordSender.sendToWriter(oneRecord);
                    }
                    consumer.commitSync();
                }
                //判断当前事件是不是0点,0点的话进程他退出
                Date date = new Date();
                if (DateUtil.targetFormat(date).split(" ")[1].substring(0, 2).equals("00")) {
                    destroy();
                }

            }
        }

        private int filterMessage(String value) {
            //如果要过滤的条件配置了
            int ifNotContinue = 0;

            if (filterContaintsStr != null) {
                String[] filterStrs = filterContaintsStr.split(",");
                //所有
                if (conditionAllOrOne == 1) {
                    //过滤掉包含filterContaintsStr的所有项的值。
                    if (filterContaintsStrFlag == 1) {
                        int i = 0;
                        for (; i < filterStrs.length; i++) {
                            if (!value.contains(filterStrs[i])) break;
                        }
                        if (i >= filterStrs.length) ifNotContinue = 1;
                    } else {
                        //留下掉包含filterContaintsStr的所有项的值
                        int i = 0;
                        for (; i < filterStrs.length; i++) {
                            if (!value.contains(filterStrs[i])) break;
                        }
                        if (i < filterStrs.length) ifNotContinue = 1;
                    }

                } else {
                    //过滤掉包含其中一项的值
                    if (filterContaintsStrFlag == 1) {
                        int i = 0;
                        for (; i < filterStrs.length; i++) {
                            if (value.contains(filterStrs[i])) break;
                        }
                        if (i < filterStrs.length) ifNotContinue = 1;
                    }
                    //留下包含其中一下的值
                    else {
                        int i = 0;
                        for (; i < filterStrs.length; i++) {
                            if (value.contains(filterStrs[i])) break;
                        }
                        if (i >= filterStrs.length) ifNotContinue = 1;
                    }
                }
            }
            return ifNotContinue;

        }

        private Record buildOneRecord(RecordSender recordSender, String value) {
            Record record = null;
            if (parsingRules.equals("regex")) {
                record = parseRegex(value, recordSender);
            } else if (parsingRules.equals("json")) {
                record = parseJson(value, recordSender);
            } else if (parsingRules.equals("split")) {
                record = parseSplit(value, recordSender);
            }
            return record;
        }

        private Record parseSplit(String value, RecordSender recordSender) {
            Record record = recordSender.createRecord();
            String[] splits = value.split(this.split);
            if (splits.length != count) {
                writerErrorPath(value);
                return null;
            }
            parseOrders(Arrays.asList(splits), record);
            return record;
        }

        private Record parseJson(String value, RecordSender recordSender) {
            Record record = recordSender.createRecord();
            HashMap<String, Object> map = JsonUtilJava.parseJsonStrToMap(value);
            System.out.println(map);
            String[] columns = kafkaReaderColumnKey.split(",");
            ArrayList<String> datas = new ArrayList<String>();
            for (String column : columns) {
                datas.add(map.get(column).toString());
                record.addColumn(new StringColumn(map.get(column).toString()));
            }
            if (datas.size() != count) {
                System.out.println(datas.size());
                writerErrorPath(value);
                return null;
            }
            parseOrders(datas, record);
            return record;
        }

        private Record parseRegex(String value, RecordSender recordSender) {
            Record record = recordSender.createRecord();
            ArrayList<String> datas = new ArrayList<String>();
            Pattern r = Pattern.compile(split);
            Matcher m = r.matcher(value);
            if (m.find()) {
                if (m.groupCount() != count) {
                    writerErrorPath(value);
                    return null;
                }
                for (int i = 1; i <= count; i++) {
                    //  record.addColumn(new StringColumn(m.group(i)));
                    datas.add(m.group(i));
                }
            } else {
                writerErrorPath(value);
                return null;
            }
            parseOrders(datas, record);
            return record;
        }

        private void writerErrorPath(String value) {
            if (exceptionPath == null) return;
            FileOutputStream fileOutputStream = null;
            try {
                fileOutputStream = getFileOutputStream();
                fileOutputStream.write((value + "\n").getBytes());
                fileOutputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private FileOutputStream getFileOutputStream() throws FileNotFoundException {
            return new FileOutputStream(exceptionPath + "/" + kafkaTopic + "errordata" + DateUtil.targetFormat(new Date(), "yyyyMMdd"), true);
        }

        private void parseOrders(List<String> datas, Record record) {
            //writerOrder
            String[] orders = writerOrder.split(",");
            for (String order : orders) {
                if (order.equals("data_from")) {
                    record.addColumn(new StringColumn(bootstrapServers + "|" + kafkaTopic));
                } else if (order.equals("uuid")) {
                    record.addColumn(new StringColumn(UUID.randomUUID().toString()));
                } else if (order.equals("null")) {
                    record.addColumn(new StringColumn("null"));
                } else if (order.equals("datax_time")) {
                    record.addColumn(new StringColumn(DateUtil.targetFormat(new Date())));
                } else if (isNumeric(order)) {
                    record.addColumn(new StringColumn(datas.get(new Integer(order) - 1)));
                }
            }
        }

        public static boolean isNumeric(String str) {
            for (int i = 0; i < str.length(); i++) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            flag = false;
        }

        public static void main(String[] args) throws IOException {
//            HashMap<String, Object> stringObjectHashMap = JsonUtilJava.parseJsonStrToMap("{\n" +
//                    "          \"id\" : \"|rsd128|20190605090628465114801\",\n" +
//                    "          \"incidentLevel\" : \"3\",\n" +
//                    "          \"reportTime\" : \"2019-06-05 03:31:52\",\n" +
//                    "          \"deviceId\" : \"09000231-c34ae3e0-97d9-4a15-860d-024ce23cf186\",\n" +
//                    "          \"dcd\" : \"DCD\",\n" +
//                    "          \"incidentTime\" : \"2019-06-05 03:31:52\",\n" +
//                    "          \"ipcId\" : \"09000231-c34ae3e0-97d9-4a15-860d-024ce23cf186\",\n" +
//                    "          \"deviceType\" : \"DCD\",\n" +
//                    "          \"incidentType\" : \"5\",\n" +
//                    "          \"incidentSubType\" : \"1\",\n" +
//                    "          \"count\" : \"1\",\n" +
//                    "          \"matchPoint\" : \"09000231-3865fc2d-7dfa-11e9-be8d-002246328334^-^RSA^192.168.1.89^00:22:46:31:4F:F2^1^A^192.168.1.2/21^0\",\n" +
//                    "          \"send_time\" : \"2019-06-05 09:06:28\",\n" +
//                    "          \"deviceIp\" : \"192.168.1.89\",\n" +
//                    "          \"deviceMac\" : \"00:22:46:31:4F:F2\"\n" +
//                    "        }");
//            System.out.println(stringObjectHashMap);
//            ArrayList<String> string = new ArrayList<String>();
//            string.add("a");
////            System.out.println(string.get(0));
//            String  str = "a^b^c^d^^d";
//            String[] split = str.split("\\^");
//            System.out.println(split.length);
            System.out.println("hello");

            FileOutputStream fileOutputStream = new FileOutputStream("bc.txt");
            fileOutputStream.write("是\n23".getBytes());
            fileOutputStream.close();
            System.out.println(fileOutputStream);
        }

    }
}
