package com.gelq.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL_Table_Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)
        ));
        DataStreamSource<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));
        //注册表
        //将流转换为表
        Table tableA = tableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));

        //将流注册为表
        tableEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

        //4.执行查询
        System.out.println(tableA);
        Table resultTable = tableEnv.sqlQuery(
                "select * from " + tableA + " where amount > 2 " +
                "union ALL " +
                "select * from OrderB where amount < 2"
        );
        //5.输出结果
        DataStream<Order> resultDS = tableEnv.toAppendStream(resultTable, Order.class);
        resultDS.print();


        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private Long user;
        private String product;
        private int amount;
    }
}
