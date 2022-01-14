package com.gelq.processAPI;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        // 从socket中获取数据
        DataStreamSource<String> inputstream = env.socketTextStream("", 7777);
        // 转换数据为SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputstream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //如果存在连续10s内温度持续上升的情况,则报警。
        dataStream.keyBy(SensorReading::getId)
                .process(new TemConsIncreWarning(Time.seconds(10).toMilliseconds())).print();

        env.execute();
    }

    private static class TemConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {
        // 报警的时间间隔(如果在interval时间内温度持续上升，则报警)
        private Long interval;
        //上一个温度值
        private ValueState<Double> lastTemperature;
        //最近一次定时器的触发时间(报警时间)
        private ValueState<Long> recentTimerStamp;

        public TemConsIncreWarning(long interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemperature", Double.class));
            recentTimerStamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("recentTimerTimeStamp", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, String>.Context context, Collector<String> collector) throws Exception {

            //当前温度值
            double curTemp = value.getTemperature();
            //上一次温度(没有则设置为当前温度)
            double lastTmep = lastTemperature.value() != null ? lastTemperature.value() : curTemp;

            //计时器状态(时间戳)
            Long timerTimestamp = recentTimerStamp.value();

            //如果当前温度 > 上次温度 并且 没有设置报警计时器,则设置
            if (curTemp > lastTmep && null == timerTimestamp) {
                long warningTimestamp = context.timerService().currentProcessingTime() + interval;
                context.timerService().registerProcessingTimeTimer(warningTimestamp);
                recentTimerStamp.update(warningTimestamp);
            }
            //如果当前温度 <= 上次温度 并且 没有设置报警计时器,则设置
            else if (curTemp <= lastTmep && null != timerTimestamp) {
                context.timerService().deleteEventTimeTimer(timerTimestamp);
                recentTimerStamp.clear();
            }

            //更新保存的温度值
            lastTemperature.update(curTemp);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发报警,并且清除 定时器状态值
            out.collect("传感器"+ctx.getCurrentKey() + "温度连续" + interval + "ms上升");
            recentTimerStamp.clear();

        }

        @Override
        public void close() throws Exception {
            lastTemperature.clear();
            recentTimerStamp.clear();
        }
    }
}
