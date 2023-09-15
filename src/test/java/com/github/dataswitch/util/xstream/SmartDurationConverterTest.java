package com.github.dataswitch.util.xstream;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class SmartDurationConverterTest {

	SmartDurationConverter c = new SmartDurationConverter();
	@Test
	public void test() {
		assertNotNull(c.fromString("p12d"));
		assertNotNull(c.fromString("PT12h"));
	}

}
