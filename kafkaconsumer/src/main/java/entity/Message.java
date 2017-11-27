package entity;

/**
 * @author trungtt
 */
public class Message
{
	private String offset;
	private String partition;

	public Message(String _offset, String _partition)
	{
		offset = _offset;
		partition = _partition;
	}

	public String getOffset()
	{
		return offset;
	}

	public void setOffset(String _offset)
	{
		offset = _offset;
	}

	public String getPartition()
	{
		return partition;
	}

	public void setPartition(String _partition)
	{
		partition = _partition;
	}
}
