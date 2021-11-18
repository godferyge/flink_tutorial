package com.gelq.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 */
public class Source_3_socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        String host = "";
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(host, 9999);

        //3.处理数据-transformation
        //3.1每一行数据按照空格切分成一个个的单词组成一个集合
        SingleOutputStreamOperator<String> flatMapResult = stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                //value就是一行行的数据
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //3.2对集合中的每个单词记为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapResult = flatMapResult.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value就是进来一个个的单词
                return Tuple2.of(value, 1);
            }
        });

        //3.3对数据按照单词(key)进行分组
//        KeyedStream<Tuple2<String, Integer>, Tuple> keyResult = mapResult.keyBy(0);
//        KeyedStream<Tuple2<String, Integer>, String> keyResult = mapResult.keyBy(t -> t.f0);
        KeyedStream<Tuple2<String, Integer>, String> keyResult = mapResult.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = keyResult.sum(1);
        sumResult.print();

        env.execute();
    }
}
