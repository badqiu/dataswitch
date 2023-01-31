package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.util.MapUtil;

public class MongodbOutputTest {

	public static String mongodbUrl = "mongodb://172.17.38.121:27017/?directConnection=true&serverSelectionTimeoutMS=2000";
	MongodbOutput output = new MongodbOutput();
	
	@Before
	public void before() throws Exception {
		output.setUrl(mongodbUrl);
		output.setDatabase("test");
		output.setCollection("badqiu_test");
	}
	
	@After
	public void after() throws Exception {
		output.close();
	}
	
	@Test
	public void testWrite() throws Exception {
		for(OutputMode outputMode : OutputMode.values()) {
			output.setPrimaryKeys("name");
			output.setOutputMode(outputMode);
			output.open(null);
			
			List rows = new ArrayList<Map>();
			for(int i = 0; i < 5; i++) {
				rows.add(MapUtil.newMap("name","badqiu-"+i,"age",20));
			}
			output.write(rows);
		}
	}

}
