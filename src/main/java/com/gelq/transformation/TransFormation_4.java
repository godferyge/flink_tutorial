package com.gelq.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormation_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> longDs = env.fromSequence(0, 100);

        //下面的操作相当于将数据随机分配一下,有可能出现数据倾斜
        SingleOutputStreamOperator<Long> filterDs = longDs.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 10;
            }
        });

        //接下来使用map操作,将数据转为(分区编号/子任务编号, 数据)
        //Rich表示多功能的,比MapFunction要多一些API可以供我们使用
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDs.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long num) throws Exception {
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDs.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        //4.sink
        //有可能出现数据倾斜
        result1.print();
        //在输出前进行了rebalance重分区平衡,解决了数据倾斜
        result2.print();

        env.execute();
    }
}
