package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.fastjson.JSONObject;
import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaProducer1 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1024);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(props);
        HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
        stringStringHashMap.put("name", "a");
        stringStringHashMap.put("id", "b");
        int i = 1;
        while (i < 100) {
            System.out.println(i);
            i++;
            stringStringKafkaProducer.send(new ProducerRecord<String, String>("testdatax", JSONObject.toJSONString(stringStringHashMap)));
        }
        //  test();

    }

    public static void test() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.7.129:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("testdatax"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList();
        String pattern = "\\<(\\d+)\\>\\^(\\S+\\s+\\d+:+\\d+:\\d+)\\^([\\w-]*)\\^(\\w+)\\^(\\S+\\s+\\d+:+\\d+:\\d+)\\^([\\w-]*)\\^(\\w+)\\^(\\d+)\\^(\\d+)\\^(\\d+)\\^([\\S+\\s]+)";
        Pattern r = Pattern.compile(pattern);
        String str = "<2>^2019-06-03 17:14:48^09000231-c34ae3e0-97d9-4a15-860d-024ce23cf186^DCD^2019-06-03 17:15:33^09000231-472759ae-801d-11e9-8617-002246328334^DCD^5^81^1^192.168.18.129^Administrator";
        Matcher m = r.matcher(str);
        System.out.println(m.groupCount());
        if (m.find()) {
            for (int i = 0; i <= 11; i++) {
                System.out.println("Found value: " + m.group(i));
            }
        }

//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            for (ConsumerRecord<String, String> record : records) {
//                String value = record.value();
//                Matcher m = r.matcher(value);
//                System.out.println(value);
//                if (m.find()) {
//                    for (int i = 0; i <= 11; i++) {
//                        System.out.println("Found value: " + m.group(i));
//                    }
//                }
//
//            }
//        }
    }

}
