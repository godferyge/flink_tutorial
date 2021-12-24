package com.gelq.window;

import com.gelq.CartInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Window_countWindow {
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

        KeyedStream<CartInfo, String> kStream = ds.keyBy(CartInfo::getSensorId);
        //需求1:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
        SingleOutputStreamOperator<CartInfo> result1 = kStream.countWindow(5).sum("count");
        //需求2:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计--基于数量的滑动窗口
        SingleOutputStreamOperator<CartInfo> result2 = kStream.countWindow(5, 3).sum("count");

//        result1.print();
        result2.print();
        env.execute();
    }
}
