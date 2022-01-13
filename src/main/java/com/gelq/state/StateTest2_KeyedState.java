package com.gelq.state;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        DataStreamSource<String> inputDstream = env.socketTextStream("", 7777);
        SingleOutputStreamOperator<SensorReading> dataStream = inputDstream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[1]));
        });

//      KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(fo -> fo.getId());
//        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        SingleOutputStreamOperator<Integer> result = dataStream.keyBy(SensorReading::getId).map(new MyMapper());
        result.print();

        env.execute();
    }


    private static class MyMapper extends RichMapFunction<SensorReading, Integer> {

        //定义一个值状态
        private ValueState<Integer> valueState;

        //定义其他状态,测试
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reducing"));


            super.open(parameters);
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            for (String str : myListState.get()) {
                System.out.println(str);
            }

            myListState.add("hello");
            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");
            // reducing state
            //            myReducingState.add(value);

            myMapState.clear();
            Integer count = valueState.value();

            count = count == null ? 0 : count;
            ++count;
            valueState.update(count);
            return count;
        }
    }
}
