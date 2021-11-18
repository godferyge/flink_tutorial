package com.gelq.source;


import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * 需求:
 *  实际开发中,经常会实时接收一些数据,要和MySQL中存储的一些规则进行匹配,那么这时候就可以使用Flink自定义数据源从MySQL中读取数据
 *  那么现在先完成一个简单的需求:
 *  从MySQL中实时加载数据
 *  要求MySQL中的数据有变化,也能被实时加载出来
 */
public class Source_5_mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStreamSource<Student> mysqlSource = env.addSource(new MysqlSource()).setParallelism(1);
        mysqlSource.print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MysqlSource extends RichParallelSourceFunction<Student> {
        private Connection conn = null;
        private PreparedStatement ps = null;

        //加载驱动,开启连接
        //Class.forName("com.mysql.jdbc.Driver");
        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "", "");
            String sql = "select id,name,age from t_student";
            ps = conn.prepareStatement(sql);

        }

        private boolean flag = true;

        @Override
        public void run(SourceContext<Student> sourceContext) throws Exception {
            ResultSet rs = ps.executeQuery();
            while (flag) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                sourceContext.collect(new Student(id, name, age));
            }
            TimeUnit.SECONDS.sleep(5);
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws SQLException {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }

    }
}
