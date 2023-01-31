package com.github.dataswitch.input;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.output.PrintOutput;
import com.github.dataswitch.util.InputOutputUtil;

public class MongodbInputTest {

	public static String mongodbUrl = "mongodb://172.17.38.121:27017/?directConnection=true&serverSelectionTimeoutMS=2000";
	
	MongodbInput input = new MongodbInput();
	
	
	@Before
	public void before() throws Exception {
		System.out.println("--------------------------------------:" + input);
		input.setUrl(mongodbUrl);
		input.setDatabase("test");
		input.setCollection("badqiu_test");
	}
	
	@After
	public void after() throws Exception {
		input.close();
	}

	@Test
	public void testInput_with_limit() throws Exception {
		input.setLimit(10);
		InputOutputUtil.copy(input, new PrintOutput());
	}
	
	@Test
	public void testInput() throws Exception {
		input.setLimit(10);
		input.setWhereJson("{\"name\":\"badqiu-0\"}");
		InputOutputUtil.copy(input, new PrintOutput());
	}
	
	@Test
	public void testInput_with_columns() throws Exception {
		input.setLimit(10);
		input.setColumns("name,count");
		InputOutputUtil.copy(input, new PrintOutput());
	}
}
