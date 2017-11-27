package consumer;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import entity.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import rebalance.HandleRebalance;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * @author trungtt
 */
public class KafkaReceiverRetry
{
	public static void main(String[] args) throws InterruptedException, IOException
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
		int partition = 0;
		List<Message> messages = readData();
		if (messages.size() > 0) {
			seekOffset = Long.valueOf(messages.get(messages.size() - 1).getOffset());
			partition = Integer.valueOf(messages.get(messages.size() - 1).getPartition());

		}

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (TopicPartition topicPartition: consumer.assignment()) {
				System.out.println("Topic: " + topicPartition.topic()  +  " Partition: " + topicPartition.partition());
			}

			if (seekOffset != -1) {
				consumer.seek(new TopicPartition("create.acc", partition), seekOffset);
			}


			Map<TopicPartition, OffsetAndMetadata> map = new HashMap<TopicPartition, OffsetAndMetadata>();
			boolean isSkip = false;
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Payload: " + record.value() + " - key: " + record.key() + " - offset: " + record.offset() + " - partition: " + record.partition());
				TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
				OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
				map.put(topicPartition, offset);
				if (record.value().equals("4") || record.value().equals("5")) {
					seekOffset = record.offset() + 1;
					partition = record.partition();
					writeData(seekOffset, partition);
					break;
				} else {
					consumer.commitSync(map);
				}


				consumer.commitSync();
			}
			Thread.sleep(2000);
		}
	}

	private static List<Message> readData() throws IOException
	{
		List<Message> messages = new ArrayList<Message>();

		String fileName = "/Users/trungtt/workspace/kafkademo/kafkaconsumer/src/main/resources/offset.csv";

		CSVReader reader = new CSVReader(new FileReader(fileName));
		String[] nextLine;

		while ((nextLine = reader.readNext()) != null) {
			for (String e: nextLine) {
				String[] option = e.split("\\.");
				messages.add(new Message(option[0], option[1]));
			}
		}
		reader.close();
		return messages;
	}

	private static void writeData(long offset, int partition) throws IOException
	{
		String record = offset + "." + partition;
		String[] data = {record};
		String fileName = "/Users/trungtt/workspace/kafkademo/kafkaconsumer/src/main/resources/offset.csv";

		CSVWriter writer = new CSVWriter(new FileWriter(fileName));
		writer.writeNext(data);
		writer.close();
	}
}
