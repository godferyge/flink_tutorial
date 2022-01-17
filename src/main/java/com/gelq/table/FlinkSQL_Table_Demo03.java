package com.gelq.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL_Table_Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<FlinkSQL_Table_Demo02.WC> input = env.fromElements(
                new FlinkSQL_Table_Demo02.WC("hello", 1),
                new FlinkSQL_Table_Demo02.WC("world", 1),
                new FlinkSQL_Table_Demo02.WC("hello", 1)
        );

        Table table = tableEnv.fromDataStream(input);


        Table resultTable = table.groupBy($("word")).select($("word"), $("frequency").sum().as("frequency")).filter($("frequency").isEqual(2));

        DataStream<Tuple2<Boolean, FlinkSQL_Table_Demo02.WC>> stream = tableEnv.toRetractStream(resultTable, FlinkSQL_Table_Demo02.WC.class);
        stream.print();


        table.printSchema();

        env.execute();
    }
}
