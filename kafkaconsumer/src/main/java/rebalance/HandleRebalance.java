package rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * @author trungtt
 */
public class HandleRebalance implements ConsumerRebalanceListener
{

	public void onPartitionsRevoked(Collection<TopicPartition> _collection)
	{
		for (TopicPartition topicPartition: _collection) {
			System.out.println("Revoke: " + topicPartition.topic() + " " + topicPartition.partition());
		}
	}

	public void onPartitionsAssigned(Collection<TopicPartition> _collection)
	{
		for (TopicPartition topicPartition: _collection) {
			System.out.println("Assign: " + topicPartition.topic() + " " + topicPartition.partition());
		}
	}
}
