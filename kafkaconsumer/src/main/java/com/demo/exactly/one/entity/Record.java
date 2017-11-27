package com.demo.exactly.one.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author trungtt
 */
@Getter
@Setter
public class Record
{
	private String payload;
	private String topic;
	private int offset;
	private int partition;

	public Record(String _payload, String _topic, int _offset, int _partition)
	{
		payload = _payload;
		topic = _topic;
		offset = _offset;
		partition = _partition;
	}
}
