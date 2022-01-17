package com.gelq.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL_Table_Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WC> input = env.fromElements(
                new WC("hello", 1),
                new WC("world", 1),
                new WC("hello", 1)
        );

        tableEnv.createTemporaryView("WordCount",input,$("word"),$("frequency"));

        Table resultTable = tableEnv.sqlQuery("select word,sum(frequency) as frequency from WordCount group by word");

        //toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate
//        DataStream<WC> resultDS = tableEnv.toAppendStream(resultTable, WC.class);

//        tableEnv.toRetractStream(resultTable,Row.class).print();

        tableEnv.toRetractStream(resultTable,WC.class).print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        private String word;
        private long frequency;
    }
}
