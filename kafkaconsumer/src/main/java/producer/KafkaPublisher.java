package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author trungtt
 */
public class KafkaPublisher
{
	public static void main(String[] args) throws InterruptedException
	{
		int partition = 0;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
//		props.put("acks", "all");
//		props.put("batch.size", 10);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i = 0; i < 10; i++) {
			if (i % 2 == 0) {
				producer.send(new ProducerRecord<String, String>("create.acc",0, Integer.toString(0), Integer.toString(i)));
			} else {
				producer.send(new ProducerRecord<String, String>("create.acc",1, Integer.toString(1), Integer.toString(i)));
			}
		}

		producer.close();
	}
}
