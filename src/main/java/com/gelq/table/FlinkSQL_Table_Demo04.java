package com.gelq.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 */
public class FlinkSQL_Table_Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        DataStreamSource<Order> orderDS = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    sourceContext.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        SingleOutputStreamOperator<Order> watermarkDS = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                        (event, timestamp) -> event.getCreateTime()
                )
        );
        //
        tableEnv.createTemporaryView("t_order", watermarkDS, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());

        tableEnv.executeSql("DESCRIBE t_order").print();

        String sql = "select\n" +
                "    userId,\n" +
                "    count(*) as totalCount,\n" +
                "    max(money) as maxMoney,\n" +
                "    min(money) as minMoney\n" +
                "from t_order\n" +
                "group by userId,tumble(createTime,interval '5' second)";
        Table resultTable = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(resultTable, Row.class).print();


        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
}
