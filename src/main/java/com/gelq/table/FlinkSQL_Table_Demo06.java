package com.gelq.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL_Table_Demo06 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TableResult inputTable = tableEnv.executeSql("create table input_kafka(\n" +
                "    user_id bigint,\n" +
                "    page_id bigint,\n" +
                "    status String\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'input_kafka',\n" +
                "    'properties.bootstrap.servers' = 'node1:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

        TableResult outputTable = tableEnv.executeSql("create table output_kafka(\n" +
                "    user_id bigint,\n" +
                "    page_id bigint,\n" +
                "    status String\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'output_kafka',\n" +
                "    'properties.bootstrap.servers' = 'node1:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka " +
                "where status = 'success'";

        Table resultTable = tableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();

        tableEnv.executeSql("insert into output_kafka select * from " + resultTable);

        env.execute();
    }
}
