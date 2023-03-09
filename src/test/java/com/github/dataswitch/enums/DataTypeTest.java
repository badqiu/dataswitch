package com.github.dataswitch.enums;

import static org.junit.Assert.*;

import org.junit.Test;

public class DataTypeTest {

	@Test
	public void test() {
		assertEquals(null,DataType.getByName(null));
		assertEquals(null,DataType.getByName(" "));
		assertEquals(DataType.STRING,DataType.getByName("stRing"));
		assertEquals(DataType.INT,DataType.getByName("int"));
	}

}
