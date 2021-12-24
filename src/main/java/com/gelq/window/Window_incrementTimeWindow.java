package com.gelq.window;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Window_incrementTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.10.7", 9999);
        SingleOutputStreamOperator<SensorReading> ds = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] values = s.split(",");
                return new SensorReading(values[0], Long.parseLong(values[1]), Double.parseDouble(values[2]));
            }
        });

        SingleOutputStreamOperator<Integer> result = ds.keyBy(SensorReading::getId)
//                .countWindow(10,2);
//        .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//        .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//        .timeWindow(Time.seconds(15)) // 已经不建议使用@Deprecated
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    //新建的累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 每个数据在上次的基础上累加
                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    // 返回结果值
                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    // 分区合并结果(TimeWindow一般用不到，SessionWindow可能需要考虑合并)
                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        result.print();
        env.execute();
    }
}
