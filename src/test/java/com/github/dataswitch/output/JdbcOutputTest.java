package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.enums.ColumnsFrom;
import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.input.JdbcInputTest;
import com.github.dataswitch.util.JdbcUtil;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.dataswitch.util.ParsedSql;
import com.github.rapid.common.util.MapUtil;

public class JdbcOutputTest {

	JdbcOutput output = new JdbcOutput();
	@Test
	public void test_sql() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setSql("insert into user (id,username) values(:id,:username)");
		output.open(new HashMap());
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}
	
	@Test
	public void test_lock_sql() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setLockSql("select * from user for update");
		output.setBeforeSql("delete from user");
		output.setSql("insert into user (id,username) values(:id,:username)");
		output.open(new HashMap());
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}

	@Test
	public void test_getReplacedSql() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where name=:name and sex=:sex");
		String replacedSql = JdbcUtil.getReplacedSql(parsedSql, MapUtil.newMap("name","badqiu","sex","m"));
		assertEquals("select * from t1 where name='badqiu' and sex='m'",replacedSql);
		System.out.println(replacedSql);
		
		
	}
	
	@Test
	public void test_getReplacedSql2() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where name=':name' and sex=':sex'");
		String replacedSql = JdbcUtil.getReplacedSql(parsedSql, MapUtil.newMap("name","badqiu","sex","m"));
		assertEquals("select * from t1 where name=':name' and sex=':sex'",replacedSql);
	}
	
	@Test
	public void test_getReplacedSql_with_same_param() {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select * from t1 where tdate=:tdate and tdate_type=:tdate_type");
		String replacedSql = JdbcUtil.getReplacedSql(parsedSql, MapUtil.newMap("tdate","tdate","tdate_type","tdate_type"));
		assertEquals("select * from t1 where tdate='tdate' and tdate_type='tdate_type'",replacedSql);
		System.out.println(replacedSql);
		
		
	}
	
	
	@Test
	public void test_table_with_auto_add_column() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setTable("user");
		output.setAutoAlterTableAddColumn(true);
		output.open(new HashMap());
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}
	
	@Test
	public void test_table_with_target_table() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setTable("user");
		output.setAutoAlterTableAddColumn(false);
		output.columnsFrom(ColumnsFrom.table);
		output.open(new HashMap());
		
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}
	
	@Test
	public void test_table_with_AutoCreateTable() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setTable("user");
		output.setAutoCreateTable(true);
		output.setAutoAlterTableAddColumn(true);
//		output.setPrimaryKeys("id");
		output.open(new HashMap());
		
		List<Object> inputRows = TestUtil.newTestDatas(20);
		output.write(inputRows);
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}
	
	@Test
	public void test_table_with_AutoCreateTable_with_drop_table() throws Exception {
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("drop table user");
		output.setTable("user");
		output.setAutoCreateTable(true);
		output.setAutoAlterTableAddColumn(true);
		output.setPrimaryKeys("id");
		output.open(new HashMap());
		
		List<Object> inputRows = TestUtil.newTestDatas(20);
		for(OutputMode outputMode : OutputMode.values()) {
			output.outputMode(outputMode);
			try {
				output.write(inputRows);
			}catch(Exception e) {
				Assert.isTrue(outputMode == OutputMode.replace,e.toString());
			}
		}
		
		for(ColumnsFrom columnsFrom : ColumnsFrom.values()) {
			output.columnsFrom(columnsFrom);
			try {
				output.write(inputRows);
			}catch(Exception e) {
				Assert.isTrue(columnsFrom == ColumnsFrom.config,e.toString());
				output.setColumns("pwd");
				output.write(inputRows);
			}
		}
		
		List<Map<String,Object>> rows = new JdbcTemplate(ds).queryForList("select * from user");
		TestUtil.printRows(rows);
		assertEquals(20,rows.size());
	}
}
