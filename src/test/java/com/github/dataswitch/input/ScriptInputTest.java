package com.github.dataswitch.input;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.dataswitch.input.ScriptInput;

public class ScriptInputTest {

	ScriptInput input = new ScriptInput();
	@Test
	public void test() {
		input.setLang("groovy");
		input.setAfterScript("System.out.println('after')");
		input.setBeforeScript("System.out.println('before'); this.beforeVar = 'beforeValue';");
		input.setScript("System.out.println(beforeVar + 'jjjjjjjjjj'); return 1;");
		
		for(int i = 0; i < 100; i++) {
			Object r = input.readObject();
			assertEquals(r,1);
		}
		
		input.close();
	}
	
}
