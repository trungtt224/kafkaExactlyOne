package com.demo.exactly.one;

import com.demo.exactly.one.dao.RecordDAO;
import com.demo.exactly.one.dao.RecordDAOImpl;
import com.demo.exactly.one.entity.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import rebalance.HandleRebalance;

import java.util.Collections;
import java.util.Properties;

/**
 * @author trungtt
 */

public class Application
{
	private static RecordDAO recordDAO;

	public static void main(String[] args)
	{
		ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");

		recordDAO = (RecordDAO) context.getBean("recordDAO");


		recordDAO.create(new Record("1", "create.acc", 1, 1));
	}

	public KafkaConsumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "kafkaReceiver");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		return consumer;
	}

	private static void processRecord(KafkaConsumer<String, String> _consumer) {
		while (true) {
			ConsumerRecords<String, String> consumerRecords = _consumer.poll(100);
			for (ConsumerRecord<String, String> consumerRecord: consumerRecords) {
				System.out.println("Payload: " + consumerRecord.value()
						+ " - key: " + consumerRecord.key()
						+ " - offset: " + consumerRecord.offset()
						+ " - partition: " + consumerRecord.partition());
				Record record = new Record(consumerRecord.value(), consumerRecord.topic(), Math.toIntExact(consumerRecord.offset()), consumerRecord.partition());
				recordDAO.create(record);
			}

		}
	}
}
