package com.gelq.state;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest1_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> inputStream = env.socketTextStream("", 9999);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        SingleOutputStreamOperator<Integer> result = dataStream.map(new MyCountMapper());

        result.print();

        env.execute();
    }

    private static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        //定义一个本地变量,作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkPointed, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
