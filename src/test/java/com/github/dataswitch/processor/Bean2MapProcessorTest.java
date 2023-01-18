package com.github.dataswitch.processor;

import org.junit.Test;

import com.github.dataswitch.BaseObject;

public class Bean2MapProcessorTest {

	Bean2MapProcessor p = new Bean2MapProcessor();
	@Test
	public void test() throws Exception {
		BaseObject row = new BaseObject();
		System.out.println(p.processOne(row));
	}

}
