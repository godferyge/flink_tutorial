package com.gelq.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.ds.print 直接输出到控制台
 * 2.ds.printToErr() 直接输出到控制台,用红色
 * 3.ds.collect 将分布式数据收集为本地集合
 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path",WriteMode.OVERWRITE)
 */
public class Sink_1_path {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.readTextFile("");

        ds.print();
        ds.printToErr();
        ds.writeAsText("", FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        env.execute();
    }
}
