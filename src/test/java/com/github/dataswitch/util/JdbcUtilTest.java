package com.github.dataswitch.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class JdbcUtilTest {

	@Test
	public void getFinalColumnKey() {
		assertEquals("some_column",JdbcUtil.getFinalColumnKey("some_column"));
		assertEquals("some_column",JdbcUtil.getFinalColumnKey(".some_column"));
		assertEquals("some_column",JdbcUtil.getFinalColumnKey("some_table.some_column"));
		assertEquals("",JdbcUtil.getFinalColumnKey("some_table."));
		assertEquals(null,JdbcUtil.getFinalColumnKey(null));
		assertEquals("   ",JdbcUtil.getFinalColumnKey("   "));
	}

}
