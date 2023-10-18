package com.github.dataswitch.output;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.TestUtil;

public class ScriptOutputTest {

	ScriptOutput output = new ScriptOutput();
	boolean debugOutputExecute = false;
	@Before
	public void before() {
		output.setOutput(new Output(){
			@Override
			public void close() throws IOException {
				
			}

			@Override
			public void write(List<Map<String, Object>> rows) {
				System.out.println("debug:"+rows);
				debugOutputExecute = true;
			}
		});
	}
	@Test
	public void test() throws Exception {
		List<Map<String,Object>> rows = TestUtil.newTestDatas(20);
		output.setLanguage("groovy");
		output.setBeforeScript("System.out.println('before executed'); this.beforeVar = ' beforeValue';");
		output.setScript("System.out.println(username+' pwd:' + pwd + beforeVar);");
		output.setAfterScript("System.out.println('after executed,')");
		output.open(null);
		output.write(rows);
		output.close();
		
		assertFalse(debugOutputExecute);
	}

	
	@Test
	public void test_with_output() throws Exception {
		
		
		List<Map<String,Object>> rows = TestUtil.newTestDatas(20);
		output.setLanguage("groovy");
		output.setBeforeScript("System.out.println('before executed'); this.beforeVar = ' beforeValue';");
		output.setScript("System.out.println(username+' pwd:' + pwd + beforeVar); output.write([username])");
		output.setAfterScript("System.out.println('after executed,')");
		output.open(null);
		output.write(rows);
		output.close();
		
		assertTrue(debugOutputExecute);
	}
}
