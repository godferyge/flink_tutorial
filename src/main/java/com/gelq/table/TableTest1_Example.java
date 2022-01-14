package com.gelq.table;

import com.gelq.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {

    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        
        env.setParallelism(1);

        // 1. 读取数据
        DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //3.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //4.基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);
        //5.调用tableAPI进行转换操作
        Table resultTable = dataTable.select("id,temperature").where("id='sensor_1'");

        //6.执行SQL
//        tableEnv.createTemporaryView("sensor",dataTable);
        tableEnv.createTemporaryView("sensor", dataStream);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";

        Table resultSqlTable = tableEnv.sqlQuery(sql);

        //7.输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
