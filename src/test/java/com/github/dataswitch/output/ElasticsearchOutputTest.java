package com.github.dataswitch.output;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.dataswitch.input.DataGenInput;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.MapUtil;

public class ElasticsearchOutputTest {

	ElasticsearchOutput output = new ElasticsearchOutput();
	
	@Test
	public void getIdFromData() throws Exception {
		output.setPrimaryKeys("name,mail");
		String v = output.getIdFromData(MapUtil.newMap("name","badqiu","mail","qq@qq.com"));
		System.out.println(v);
		assertEquals("badqiu_qq@qq.com",v);
	}

	@Test
	public void getIdFromData2() throws Exception {
		output.setPrimaryKeys("name");
		String v = output.getIdFromData(MapUtil.newMap("name","badqiu","mail","qq@qq.com"));
		System.out.println(v);
		assertEquals("badqiu",v);
	}
	
	@Test
	public void getIdFromData_empty() throws Exception {
		output.setPrimaryKeys(null);
		String v = output.getIdFromData(MapUtil.newMap("name","badqiu","mail","qq@qq.com"));
		System.out.println(v);
		assertEquals(null,v);
	}
	
	@Test
	public void testWrite() {
		output.setIndex("test_demo_index");
		output.setHosts(System.getenv("TEST_ES_HOSTS"));
		output.setUsername(System.getenv("TEST_ES_USERNAME"));
		output.setPassword(System.getenv("TEST_ES_PASSWORD"));
		DataGenInput input = new DataGenInput(1000);
		
		InputOutputUtil.openAll(input,output);
		InputOutputUtil.copy(input, output);
		
	}
}
