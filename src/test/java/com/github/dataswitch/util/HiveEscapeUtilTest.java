package com.github.dataswitch.util;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

public class HiveEscapeUtilTest {

	@Test
	public void test() {
		Assert.assertEquals(null,HiveEscapeUtil.hiveEscaped(null));
		Assert.assertEquals("a\\nb",HiveEscapeUtil.hiveEscaped("a\nb"));
		Assert.assertEquals("a\\001b",HiveEscapeUtil.hiveEscaped("a\1b"));
		Assert.assertEquals("a\\002b",HiveEscapeUtil.hiveEscaped("a\002b"));
		Assert.assertEquals("a\\003b",HiveEscapeUtil.hiveEscaped("a\3b"));
		Assert.assertEquals("a\\n\\n\\003b",HiveEscapeUtil.hiveEscaped("a\n\n\3b"));
	}

}
