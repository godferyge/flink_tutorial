package com.gelq.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 *  使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 */
public class FlinkSQL_Table_Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<FlinkSQL_Table_Demo04.Order> orderDS = env.addSource(new RichSourceFunction<FlinkSQL_Table_Demo04.Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<FlinkSQL_Table_Demo04.Order> sourceContext) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    FlinkSQL_Table_Demo04.Order order = new FlinkSQL_Table_Demo04.Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    sourceContext.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        SingleOutputStreamOperator<FlinkSQL_Table_Demo04.Order> watermarkDS = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<FlinkSQL_Table_Demo04.Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
        );
        //4.注册表
        tableEnv.createTemporaryView("t_order", watermarkDS,
                $("orderId"), $("userId"), $("money"), $("createTime").rowtime());

        //查看表约束
        tableEnv.from("t_order").printSchema();

        //5.TableAPI查询
        Table resultTable = tableEnv.from("t_order");


        Table resultTable2 = resultTable.window(Tumble.over(lit(5).second())
                        .on($("createTime"))
                        .as("tumbleWindow"))
                .groupBy($("tumbleWindow"), $("userId"))
                .select(
                        $("userId"),
                        $("userId").count().as("totalCount"),
                        $("money").max().as("maxMoney"),
                        $("money").min().as("minMoney")
                );

        tableEnv.toRetractStream(resultTable2, Row.class).print();

        env.execute();
    }
}
