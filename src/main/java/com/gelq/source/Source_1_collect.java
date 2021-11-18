package com.gelq.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 把本地的普通的Java集合/Scala集合变为分布式的Flink的DataStream集合!
 * 一般用于学习测试时编造数据时使用
 * 1.env.fromElements(可变参数);
 * 2.env.fromColletion(各种集合);
 * 3.env.generateSequence(开始,结束);
 * 4.env.fromSequence(开始,结束);
 */
public class Source_1_collect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStreamSource<String> ds2 = env.fromCollection(Arrays.asList("hadoop", "spark", "flink"));
        DataStreamSource<Long> ds3 = env.generateSequence(1, 10);
        DataStreamSource<Long> ds4 = env.fromSequence(1, 10);

        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
