package com.gelq.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Source_6_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "slave01.paic.com.cn:6667");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties);
        //设置从最早的offset消费
        kafkaConsumer.setStartFromEarliest();

        //还可以手动指定相应的 topic, partition，offset,然后从指定好的位置开始消费
        //HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
        //map.put(new KafkaTopicPartition("test", 1), 10240L);
        //假如partition有多个，可以指定每个partition的消费位置
        //map.put(new KafkaTopicPartition("test", 2), 10560L);
        //然后各个partition从指定位置消费
        //consumer.setStartFromSpecificOffsets(map);

        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);


        dataStreamSource.print();
        env.execute();
    }
}
