package com.github.dataswitch.output;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.output.RedisOutput;

public class RedisOutputTest {

	RedisOutput out = new RedisOutput();
	@Test
	public void test() {
		out.write(TestUtil.newTestDatas(10));
	}

}
