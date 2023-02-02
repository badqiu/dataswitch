package com.github.dataswitch.input;

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
	}
	
	@After
	public void after() throws Exception {
		input.close();
	}

	
	@Test
	public void testInput() throws Exception {
		InputOutputUtil.copy(input, new PrintOutput());
	}
	


}
