package com.gelq.window;

import com.gelq.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Window_fullWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 从socket文本流获取数据
        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.10.7", 9999);

        // 转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> ds = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] values = s.split(",");
                return new SensorReading(values[0], Long.parseLong(values[1]), Double.parseDouble(values[2]));
            }
        });
        // 2. 全窗口函数 （WindowFunction和ProcessWindowFunction，后者更全面）
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = ds.keyBy(s -> s.getId()).window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {
//
//                    }
//                })
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });
        apply.print();

        env.execute();
    }
}
