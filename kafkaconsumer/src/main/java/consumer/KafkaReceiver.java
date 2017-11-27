package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import rebalance.HandleRebalance;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author trungtt
 */
public class KafkaReceiver
{
	public static void main(String[] args) throws InterruptedException
	{
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "kafkaReceiver");
		props.put("enable.auto.commit", "false");
//		props.put("auto.commit.interval.ms", "101");
//		props.put("max.partition.fetch.bytes", "135");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList("create.acc"), new HandleRebalance());
		long seekOffset = -1;

		while (true) {
//			if (seekOffset != -1) {
//				consumer.seek(new TopicPartition("create.acc",1), seekOffset);
//			}

			ConsumerRecords<String, String> records = consumer.poll(100);
			Map<TopicPartition, OffsetAndMetadata> map = new HashMap<TopicPartition, OffsetAndMetadata>();
			boolean isSkip = false;
			xxx:for (ConsumerRecord<String, String> record: records) {
				System.out.println("Payload: " + record.value() + " - key: " + record.key() + " - offset: " + record.offset() + " - partition: " + record.partition());
//				TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
//				OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
//				map.put(topicPartition, offset);
//				if (record.value().equals("4")) {
//					isSkip = true;
//				}
//
//				if (! isSkip) {
//					seekOffset = record.offset() + 1;
//					consumer.commitSync(map);
//				}
				consumer.commitSync();
			}
			Thread.sleep(2000);
 		}
	}
}
