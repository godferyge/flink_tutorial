package com.gelq.connectors;

import com.gelq.sink.Sink_1_mysql.Student;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Connectors_1_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Student> tonyma = env.fromElements(new Student(null, "tonyma", 18));
        tonyma.addSink(JdbcSink.sink("INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)", new JdbcStatementBuilder<Student>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Student student) throws SQLException {
                preparedStatement.setString(1,student.getName());
                preparedStatement.setInt(2,student.getAge());
            }
        },new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://")
                .withPassword("")
                .withUsername("")
                .withDriverName("com.mysql.jdbc.driver")
                .build()));

        env.execute();
    }
}
