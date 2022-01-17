package com.gelq.table;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> inputStream = env.readTextFile("");

        // 3. 转换成POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 4. 将流转换成表，定义时间特性
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,pt.proctime");


        dataTable.printSchema();

        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }
}
