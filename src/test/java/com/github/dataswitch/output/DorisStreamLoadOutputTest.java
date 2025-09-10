package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DorisStreamLoadOutputTest {

	@Test
	public void test() {
		String seperator = "\001";
		System.out.println(seperator);
		System.out.println(DorisStreamLoadOutput.escapeInvisibleChars(seperator));
		assertEquals("\\x01",seperator);
	}

}
