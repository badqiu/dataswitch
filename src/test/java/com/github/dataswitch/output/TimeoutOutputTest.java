package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimeoutOutputTest {

	TimeoutOutput output = new TimeoutOutput();
	@Test
	public void setTimeout() {
		long hour = 3600 * 1000;
		long minute = 60 * 1000;
		long second =  1000;
		
		output.setTimeout("3s");
		assertEquals(3 * second,output.getTimeout());
		
		output.setTimeout("2m");
		assertEquals(120 * second,output.getTimeout());
		
		output.setTimeout("2h");
		assertEquals(2 * hour,output.getTimeout());
		
		output.setTimeout("90m");
		assertEquals(90 * minute,output.getTimeout());
		
		output.setTimeout("90m30s");
		assertEquals(90 * minute + 30 * second,output.getTimeout());
		
		output.setTimeout("5h30m30s");
		assertEquals(5 * hour + 30 * minute + 30 * second,output.getTimeout());
		
	}

}
