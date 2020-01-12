import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * checkpoint机制
 * <p>
 * Exactly-Once 语义
 */
public class checkpoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

        DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
                kafkaUtil.getTopic(), new SimpleStringSchema(), properties
        ));

        appInfoSource.keyBy((KeySelector<String, String>) appId -> appId)
                .map(new RichMapFunction<String, Tuple2<String, Long>>() {
                    private ValueState<Long> valueState;
                    private long value = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pvState",
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })));
                    }

                    @Override
                    public Tuple2<String, Long> map(String appId) throws Exception {
                        if (null == valueState.value()) {
                            value = 1;
                        } else {
                            value = valueState.value() + 1;
                        }
                        valueState.update(value);
                        return new Tuple2<>(appId, value);
                    }
                })
                .print();
        env.execute("check");
    }
}
