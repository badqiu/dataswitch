package com.github.dataswitch.output;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.input.JdbcInputTest;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.dataswitch.util.ParsedSql;
import com.github.rapid.common.util.MapUtil;

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
	
	@Test
	public void test_lock_sql() {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setLockSql("select * from user");
		output.setBeforeSql("delete from user");
		output.setSql("insert into user (id,username) values(:id,:username)");
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}

	@Test
	public void test_getReplacedSql() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where name=:name and sex=:sex");
		String replacedSql = JdbcOutput.getReplacedSql(parsedSql, MapUtil.newMap("name","badqiu","sex","m"));
		assertEquals("select * from t1 where name='badqiu' and sex='m'",replacedSql);
		System.out.println(replacedSql);
		
		
	}
	
	@Test
	public void test_getReplacedSql2() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where name=':name' and sex=':sex'");
		String replacedSql = JdbcOutput.getReplacedSql(parsedSql, MapUtil.newMap("name","badqiu","sex","m"));
		assertEquals("select * from t1 where name=':name' and sex=':sex'",replacedSql);
	}
	
	@Test
	public void test_getReplacedSql_with_same_param() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where tdate=:tdate and tdate_type=:tdate_type");
		String replacedSql = JdbcOutput.getReplacedSql(parsedSql, MapUtil.newMap("tdate","tdate","tdate_type","tdate_type"));
		assertEquals("select * from t1 where tdate='tdate' and tdate_type='tdate_type'",replacedSql);
		System.out.println(replacedSql);
		
		
	}
	
}
