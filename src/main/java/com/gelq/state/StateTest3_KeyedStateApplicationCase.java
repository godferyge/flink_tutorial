package com.gelq.state;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("", 9999);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[1]));
        });


        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = dataStream.keyBy(SensorReading::getId).flatMap(new MyFlatMapper(10.0));

        result.print();

        env.execute();
    }

    // 如果 传感器温度 前后差距超过指定温度(这里指定10.0),就报警
    private static class MyFlatMapper extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //报警的温差阈值
        private final Double threshold;

        //记录上一次的温度
        ValueState<Double> lastTemperature;

        private MyFlatMapper(Double threshold) {
            this.threshold = threshold;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            //从运行时上下文中获取KeyedState
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));

        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double curTemp = sensorReading.getTemperature();

            // 如果不为空，判断是否温差超过阈值，超过则报警
            if (lastTemp != null) {
                if (Math.abs(curTemp - lastTemp) >= threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, curTemp));
                }

            }

            lastTemperature.update(curTemp);
        }

        @Override
        public void close() throws Exception {
            lastTemperature.clear();
        }
    }
}
