import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class kafkaUtil {
    private static final String broker = "localhost:9093";
    private static final String topic = "test";

    //统计不同数据的个数
    private static final HashMap<String, Long> producerMap = new HashMap<>();

    private static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        String value = String.valueOf(new Random().nextInt(2));
        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, value);
        producer.send(record);
        System.out.println("发送数据");
        Long pv = producerMap.get(value);
        if (pv == null) {
            producerMap.put(value, 1L);
        } else {
            producerMap.put(value, pv + 1);
        }
        System.out.println("接收数据");
        for (Map.Entry<String, Long> appId : producerMap.entrySet()) {
            System.out.println("appId: " + appId.getKey() + " count: " + appId.getValue());
        }

        //producer.flush();

    }

    public static String getTopic() {
        return topic;
    }

    public static void main(String[] args) {
        while (true) {
            try {
                Thread.sleep(1000);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}
