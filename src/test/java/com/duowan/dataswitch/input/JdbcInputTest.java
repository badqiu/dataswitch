package com.duowan.dataswitch.input;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.duowan.dataswitch.TestUtil;
import com.mchange.v2.c3p0.DriverManagerDataSource;

public class JdbcInputTest {

	JdbcInput input = new JdbcInput();
	@Test
	public void test() throws IOException {
		DriverManagerDataSource ds = createDataSourceAndInsertData();
		
		input.setDataSource(ds);
		input.setSql("select * from user");
		input.init();
		
		int count = TestUtil.printInputReadRows(input);
		assertEquals(6,count);
	}
	
	public static DriverManagerDataSource createDataSourceAndInsertData() {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setUser("sa");
		ds.setPassword("");
		ds.setJdbcUrl("jdbc:h2:mem:object_sql_query;MODE=MYSQL;DB_CLOSE_DELAY=-1");
		ds.setDriverClass("org.h2.Driver");
		
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
