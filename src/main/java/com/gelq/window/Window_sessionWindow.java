package com.gelq.window;

import com.gelq.CartInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Window_sessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.10.7", 9999);
        SingleOutputStreamOperator<CartInfo> ds = sourceStream.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String s) throws Exception {
                String[] values = s.split(",");
                return new CartInfo(values[0], Integer.parseInt(values[1]));
            }
        });

        KeyedStream<CartInfo, String> kStream = ds.keyBy(line -> line.getSensorId());


        //设置会话超时时间为10s,10s内没有数据上来,则触发上个窗口的计算.
        SingleOutputStreamOperator<CartInfo> result = kStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");

        result.print();

        env.execute();
    }
}
