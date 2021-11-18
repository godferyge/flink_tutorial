package com.gelq.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * env.readTextFile(本地/HDFS文件/文件夹);
 * 压缩文件也可以
 *
 */
public class Source_2_file {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds1 = env.readTextFile("data/input/words.txt");
        DataStreamSource<String> ds2 = env.readTextFile("data/input/dir");
        DataStream<String> ds3 = env.readTextFile("hdfs://node1:8020//wordcount/input/words.txt");
        DataStream<String> ds4 = env.readTextFile("data/input/wordcount.txt.gz");

        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        env.execute();
    }
}
