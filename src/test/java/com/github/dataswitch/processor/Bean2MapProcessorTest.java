package com.github.dataswitch.processor;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class Bean2MapProcessorTest {

	Bean2MapProcessor p = new Bean2MapProcessor();
	@Test
	public void test() throws Exception {
		Map row = new HashMap();
		System.out.println(p.processOne(row));
	}

}
