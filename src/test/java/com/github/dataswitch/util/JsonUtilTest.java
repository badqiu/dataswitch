package com.github.dataswitch.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class JsonUtilTest {

	@Test
	public void test() {
		System.out.println(JsonUtil.parseJson(""));
		System.out.println(JsonUtil.parseJson("[]"));
		System.out.println(JsonUtil.parseJson("[]"));
	}

}
