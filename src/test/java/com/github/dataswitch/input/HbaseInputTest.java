package com.github.dataswitch.input;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.output.HbaseOutputTest;
import com.github.dataswitch.output.PrintOutput;
import com.github.dataswitch.util.InputOutputUtil;

public class HbaseInputTest {

	public static String mongodbUrl = "mongodb://172.17.38.121:27017/?directConnection=true&serverSelectionTimeoutMS=2000";
	
	HbaseInput input = new HbaseInput();
	
	
	@Before
	public void before() throws Exception {
		System.out.println("--------------------------------------:" + input);
		input.setHbaseConfig(HbaseOutputTest.hbaseConfig);
		input.setTable("test_hbase_output");
		input.setFamily("f");
		
		input.setColumnsTypeByBean(TestHbaseInputBean.class);
	}
	
	@After
	public void after() throws Exception {
		input.close();
	}

	
	@Test
	public void testInput() throws Exception {
		InputOutputUtil.copy(input, new PrintOutput());
	}
	
	
	public static class TestHbaseInputBean {
		private String name;
		private Date time;
		private char count;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Date getTime() {
			return time;
		}
		public void setTime(Date time) {
			this.time = time;
		}
		public char getCount() {
			return count;
		}
		public void setCount(char count) {
			this.count = count;
		}
		
		
	}


}
