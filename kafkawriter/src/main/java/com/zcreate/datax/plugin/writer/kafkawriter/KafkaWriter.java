package com.zcreate.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: KafkaWriter
 * @Author: nkong
 * @CreateDate: 2020/10/10 11:06
 * @Version: 1.0
 * @Description: datax kafkawriter
 */

public class KafkaWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            logger.info("kafka writer params:{}", conf.toJSON());
            this.validateParameter();
        }

        private void validateParameter() {
            this.conf.getNecessaryValue(Key.BOOTSTRAP_SERVERS, KafkaWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tmpConfig = conf.clone();
                configurations.add(tmpConfig);
            }
            return configurations;
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Producer<String, String> producer;
        private String fieldDelimiter;
        private Configuration conf;
        private Properties props;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);

            props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.BOOTSTRAP_SERVERS));
            props.put("acks", conf.getUnnecessaryValue(Key.ACK, "0",
                null));//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
            props.put("retries", conf.getUnnecessaryValue(Key.RETRIES, "0", null));
            //控制发送者在发布到kafka之前等待批处理的字节数。 满足batch.size和ling.ms之一，producer便开始发送消息
            //默认16kb
            props.put("batch.size", conf.getUnnecessaryValue(Key.BATCH_SIZE, "16384", null));
            props.put("linger.ms", 1);
            props.put("key.serializer", conf.getUnnecessaryValue(Key.KEYSERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer", null));
            props.put("value.serializer", conf.getUnnecessaryValue(Key.VALUESERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer", null));
            producer = new KafkaProducer<String, String>(props);
        }

        @Override
        public void prepare() {
            ListTopicsResult topicsResult = AdminClient.create(props).listTopics();
            String topic = conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);

            try {
                if (!topicsResult.names().get().contains(topic)) {
                    NewTopic newTopic = new NewTopic(
                        topic,
                        Integer.valueOf(
                            conf.getUnnecessaryValue(Key.TOPIC_NUM_PARTITION, "1", null)),
                        Short.valueOf(
                            conf.getUnnecessaryValue(Key.TOPIC_REPLICATION_FACTOR, "1", null))
                    );
                    AdminClient adminClient = AdminClient.create(props);
                    adminClient.createTopics(Lists.newArrayList(newTopic));
                    adminClient.close();
                }
            } catch (Exception e) {
                throw new DataXException(KafkaWriterErrorCode.CREATE_TOPIC,
                    KafkaWriterErrorCode.REQUIRED_VALUE.getDescription());
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            logger.info("start to writer kafka");
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                if (conf.getString(Key.WRITE_TYPE, WriteType.TEXT.name())
                    .equalsIgnoreCase(WriteType.TEXT.name())) {
                    producer.send(new ProducerRecord<String, String>(this.conf.getString(Key.TOPIC),
                        recordToString(record)));
                } else if (conf.getString(Key.WRITE_TYPE, WriteType.TEXT.name())
                    .equalsIgnoreCase(WriteType.JSON.name())) {
                    producer.send(new ProducerRecord<String, String>(this.conf.getString(Key.TOPIC),
                        record.toString()));
                }
                producer.flush();
            }
        }

        @Override
        public void destroy() {
            if (producer != null) {
                producer.close();
            }
        }

        private String recordToJson(Record record) {

            return null;
        }

        /**
         * 数据格式化
         */
        private String recordToString(Record record) {
            int recordColumnNumber = record.getColumnNumber();
            if (0 == recordColumnNumber) {
                return NEWLINE_FLAG;
            }
            Column column;
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < recordColumnNumber; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }
    }
}
