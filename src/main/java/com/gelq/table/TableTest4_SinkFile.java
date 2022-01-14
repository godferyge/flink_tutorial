package com.gelq.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest4_SinkFile {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "/tmp/Flink_Tutorial/src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())).createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id,temp").filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id,id.count as count,temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'senosr_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

//        // 打印输出
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
//        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");

        //4.输出到文件
        //连接外部文件注册输出表
        tableEnv.connect(new FileSystem().path(""))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        配合 aggTable.insertInto("outputTable"); 才使用下面这条
//                        .field("cnt",DataTypes.INT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
        // 这条会报错(文件系统输出，不支持随机写，只支持附加写)
        // Exception in thread "main" org.apache.flink.table.api.TableException:
        // AppendStreamTableSink doesn't support consuming update changes which is produced by
        // node GroupAggregate(groupBy=[id], select=[id, COUNT(id) AS EXPR$0, AVG(temp) AS EXPR$1])
        //        aggTable.insertInto("outputTable");

        // 旧版可以用下面这条
        //        env.execute();

        // 新版需要用这条，上面那条会报错，报错如下
        // Exception in thread "main" java.lang.IllegalStateException:
        // No operators defined in streaming topology. Cannot execute.
        tableEnv.execute("");
    }
}
