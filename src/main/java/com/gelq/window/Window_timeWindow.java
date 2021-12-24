package com.gelq.window;

import com.gelq.CartInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Window_timeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

//ss
        DataStreamSource<String> dataSource = env.socketTextStream("192.168.10.7", 9999);
        SingleOutputStreamOperator<CartInfo> carDS = dataSource.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String lines) throws Exception {
                String[] words = lines.split(",");
                 return new CartInfo(words[0], Integer.valueOf(words[1]));
            }
        });

        //注意: 需求中要求的是各个路口/红绿灯的结果,所以需要先分组
//        KeyedStream<CartInfo, String> kStream = carDS.keyBy(cartInfo -> cartInfo.getSensorId());
//        KeyedStream<CartInfo, String> kStream = carDS.keyBy(new KeySelector<CartInfo, String>() {
//            @Override
//            public String getKey(CartInfo cartInfo) throws Exception {
//                return cartInfo.getSensorId();
//            }
//        });

        KeyedStream<CartInfo, String> kStream = carDS.keyBy(CartInfo::getSensorId);
        // * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
//        WindowedStream<CartInfo, String, TimeWindow> windowStream = kStream.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<CartInfo> result1 = kStream.window(TumblingProcessingTimeWindows.of(Time.minutes(5))).sum("count");
        // * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
        SingleOutputStreamOperator<CartInfo> result2 = kStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("count");


        result1.print("result1");
        result2.print("result2");

        env.execute();
    }
}
