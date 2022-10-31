package com.github.dataswitch.input;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.github.dataswitch.TestUtil;

public class JdbcInputTest {

	JdbcInput input = new JdbcInput();
	@Test
	public void test() throws Exception {
		DriverManagerDataSource ds = createDataSourceAndInsertData();
		
		input.setDataSource(ds);
		input.setSql("select * from user");
		input.init();
		
		int count = TestUtil.printInputReadRows(input);
		assertEquals(6,count);
	}
	
	@Test
	public void test_setMapKey2lowerCase() throws Exception {
		DriverManagerDataSource ds = createDataSourceAndInsertData();
		
		input.setDataSource(ds);
		input.setSql("select * from user");
		input.setMapKey2lowerCase(false);
		input.init();
		
		int count = TestUtil.printInputReadRows(input);
		assertEquals(6,count);
	}
	
	static int dbCount = 0;
	public static DriverManagerDataSource createDataSourceAndInsertData() {
		dbCount++;
		DriverManagerDataSource ds = new DriverManagerDataSource();
		
		ds.setUsername("sa");
		ds.setPassword("");
		ds.setUrl("jdbc:h2:mem:object_sql_query_"+dbCount+";MODE=MYSQL;DB_CLOSE_DELAY=-1");
		ds.setDriverClassName("org.h2.Driver");
		
		new JdbcTemplate(ds).execute("create table user(id int primary key,username varchar(20),password varchar(20),age int,sex varchar(1))");
		new JdbcTemplate(ds).execute("insert into user values(1,'badqiu','123',20,'M')");
		new JdbcTemplate(ds).execute("insert into user values(2,'jane','123',21,'f')");
		new JdbcTemplate(ds).execute("insert into user values(3,'ddd','123',21,'m')");
		new JdbcTemplate(ds).execute("insert into user values(4,'badqiu2','123',20,'M')");
		new JdbcTemplate(ds).execute("insert into user values(5,'jane2','123',21,'f')");
		new JdbcTemplate(ds).execute("insert into user values(6,'ddd2','123',21,'m')");
		return ds;
	}

}
