package com.duowan.dataswitch.output;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.duowan.dataswitch.TestUtil;
import com.duowan.dataswitch.input.JdbcInputTest;

public class JdbcOutputTest {

	JdbcOutput output = new JdbcOutput();
	@Test
	public void test() {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setSql("insert into user (id,username) values(:id,:username)");
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}

}
