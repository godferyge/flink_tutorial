package com.gelq.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 将两个String类型的流进行union
 * 将一个String类型和一个Long类型的流进行connect
 */
public class TransFormation_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStream<String> ds2 = env.fromElements("hadoop", "spark", "flink");
        DataStream<Long> ds3 = env.fromElements(1L, 2L, 3L);

        DataStream<String> result1 = ds1.union(ds2);
        ConnectedStreams<String, Long> tempResult = ds1.connect(ds3);


        SingleOutputStreamOperator<String> result2 = tempResult.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "String->String:" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long->String:" + value.toString();
            }
        });


        result1.print("result1");
        result2.print("result2");

        env.execute();
    }
}
