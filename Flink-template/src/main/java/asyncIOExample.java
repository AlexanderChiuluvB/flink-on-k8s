import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class asyncIOExample {
    private static final String EXACTLY_ONCE_MODE = "exactly_once";
    private static final String EVENT_TIME = "EventTime";
    private static final String INGESTION_TIME = "IngestionTime";
    private static final String ORDERED = "ordered";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String statePath;
        final String cpMode;
        final int maxCount;
        final long sleepFactor;
        final float failRatio;
        final String mode;
        final int taskNum;
        final String timeType;
        final long shutdownWaitTS;
        final long timeout;

        try {
            // check the configuration for the job
            statePath = params.get("fsStatePath", null);
            cpMode = params.get("checkpointMode", "exactly_once");
            maxCount = params.getInt("maxCount", 100000);
            sleepFactor = params.getLong("sleepFactor", 100);
            failRatio = params.getFloat("failRatio", 0.001f);
            mode = params.get("waitMode", "ordered");
            taskNum = params.getInt("waitOperatorParallelism", 1);
            timeType = params.get("eventType", "EventTime");
            shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
            timeout = params.getLong("timeout", 10000L);
        } catch (Exception e) {
            throw e;
        }

        if (statePath != null) {
            env.setStateBackend(new FsStateBackend(statePath));
        }

        if (EXACTLY_ONCE_MODE.equals(cpMode)) {
            env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
        }

        if (EVENT_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else if (INGESTION_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));

        //创建异步函数
        AsyncFunction<Integer, String> function =
                new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);

        //异步IO作为操作算子
        DataStream<String> result;
        if (ORDERED.equals(mode)) {
            result = AsyncDataStream.orderedWait(
                    inputStream,
                    function,
                    timeout,
                    TimeUnit.MILLISECONDS,
                    20
            ).setParallelism(taskNum);
        } else {
            result = AsyncDataStream.unorderedWait(
                    inputStream,
                    function,
                    timeout,
                    TimeUnit.MILLISECONDS,
                    20
            ).setParallelism(taskNum);
        }

        result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(s, 1));
            }
        })
                .keyBy(0)
                .sum(1)
                .print();
        env.execute();
    }

    private static class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {

        private volatile boolean isRunning = true;
        private int counter = 0;
        private int start = 0;

        public SimpleSource(int maxNum) {
            this.counter = maxNum;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(start);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer i : state) {
                this.start = i;
            }
        }

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while ((start < counter || counter == -1) && isRunning) {
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(start);
                    ++start;

                    if (start == Integer.MAX_VALUE) {
                        start = 0;
                    }
                }
                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {

        private transient ExecutorService executorService;

        // 线程池暂停工作时间,用于模拟一个耗时的异步操作
        private long sleepFactor;

        // 模拟一个出错的IO请求
        private float failRatio;

        private long shutdownWaitTS;

        public SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
            this.sleepFactor = sleepFactor;
            this.failRatio = failRatio;
            this.shutdownWaitTS = shutdownWaitTS;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            executorService = Executors.newFixedThreadPool(30);

        }

        @Override
        public void close() throws Exception {
            super.close();
            ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
        }

        @Override
        public void asyncInvoke(Integer integer, ResultFuture<String> resultFuture) throws Exception {
            executorService.submit(() -> {
                long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
                try {
                    Thread.sleep(sleep);
                    if (ThreadLocalRandom.current().nextFloat() < failRatio) {
                        resultFuture.completeExceptionally(new Exception("failed"));
                    } else {
                        resultFuture.complete(Collections.singletonList("key-" + (integer %10)));
                    }
                } catch (InterruptedException e) {
                    resultFuture.complete(new ArrayList<>(0));
                }
            });
        }
    }

}
