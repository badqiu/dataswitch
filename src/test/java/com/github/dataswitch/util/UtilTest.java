package com.github.dataswitch.util;

import static org.junit.Assert.*;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class UtilTest {

	@Test
	public void test() {
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user,age"),";"));
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user  age"),";"));
		assertEquals("user;age;diy;blog;abc",StringUtils.join(Util.splitColumns("user\nage\n\t\ndiy\nblog,abc"),";"));
	}

}
