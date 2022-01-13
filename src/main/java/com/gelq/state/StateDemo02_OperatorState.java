package com.gelq.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class StateDemo02_OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //先直接使用下面的代码设置CheckPoint时间间隔和磁盘路径以及代码遇到异常后的重启策略.
        env.enableCheckpointing(1000);//每隔1s执行一次Checkpoint
        env.setStateBackend(new FsStateBackend("C:\\pingan\\IdeaProject\\flink_tutorial\\src\\main\\resources\\ckp"));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //固定延迟重启策略:程序出现异常的时候,重启两次,每次延迟3秒钟重启,超过2次,程序退出.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));


        DataStreamSource<String> stringDataStreamSource = env.addSource(new MyKafkaSource());
        stringDataStreamSource.print();
        env.execute();
    }

    //MyKafkaSource就是模拟的FlinkKafkaConsumer并维护offset
    private static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        //1.声明一个OperatorState 来记录offset
        private ListState<Long> offSetState = null;
        private Long offset = 0L;
        private boolean flag = true;

        //下面的snapshotState方法会按照固定的时间间隔将State信息存储到Checkpoint/磁盘中,也就是磁盘做快照
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //5.保存State到checkpoint中
            offSetState.clear();//清理内存中存储的offset到CheckPoint中
            //将offset存入State中.
            offSetState.add(offset);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //2.创建状态描述器
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offSetState", Long.class);

            //3.根据状态描述器初始化状态
            offSetState = context.getOperatorStateStore().getListState(descriptor);


        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            //4.获取并使用State中的值
            Iterator<Long> iterator = offSetState.get().iterator();
            if (iterator.hasNext()) {
                offset = iterator.next();
            }

            while (flag) {
                offset += 1;
                int id = getRuntimeContext().getIndexOfThisSubtask();
                ctx.collect("分区:" + id + "消费到的offset位置为:" + offset);
                TimeUnit.SECONDS.sleep(2);
                if (offset % 5 == 0) {
                    System.out.println("程序遇到异常了......");
                    throw new Exception("程序遇到异常了......");
                }
            }

        }

        @Override
        public void cancel() {
            flag = false;

        }
    }
}
