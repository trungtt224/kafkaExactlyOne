package com.demo.exactly.one.dao;

import com.demo.exactly.one.entity.Record;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.sql.ResultSet;

/**
 * @author trungtt
 */
public class RecordDAOImpl implements RecordDAO
{
	private DataSource dataSource;
	private JdbcTemplate jdbcTemplate;

	public void setDataSource(DataSource _dataSource)
	{
		dataSource = _dataSource;
		this.jdbcTemplate = new JdbcTemplate(_dataSource);
	}

	@Override
	public void create(Record _record)
	{
		String query = "INSERT INTO record(payload, topic, record_offset, partition) VALUES(?,?,?,?)";
		jdbcTemplate.update(query, _record.getPayload(), _record.getTopic(), _record.getOffset(), _record.getPartition());
	}

	private static RowMapper<Record> mapRow()
	{
		return (ResultSet rs, int rowNum) -> {
			String payload = rs.getString("payload");
			String topic = rs.getString("topic");
			int offset = rs.getInt("record_offset");
			int partition = rs.getInt("partition");
			Record record = new Record(payload, topic, offset, partition);
			return record;
		};
	}
}
