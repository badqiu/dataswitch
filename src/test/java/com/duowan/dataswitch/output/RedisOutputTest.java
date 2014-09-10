package com.duowan.dataswitch.output;

import static org.junit.Assert.*;

import org.junit.Test;

import com.duowan.dataswitch.TestUtil;

public class RedisOutputTest {

	RedisOutput out = new RedisOutput();
	@Test
	public void test() {
		out.write(TestUtil.newTestDatas(10));
	}

}
