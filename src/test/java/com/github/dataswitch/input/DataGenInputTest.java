package com.github.dataswitch.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.github.dataswitch.output.NullOutput;
import com.github.dataswitch.output.PrintOutput;
import com.github.dataswitch.output.StatOutput;
import com.github.dataswitch.util.InputOutputUtil;

public class DataGenInputTest {

	DataGenInput input = new DataGenInput();
	
	@Test
	public void setRowsPerSecond_Max() {
		long rowsLimit = 1000000;
		input.setRowsPerSecond(100000);
		input.setRowsLimit(rowsLimit);
		StatOutput output = new StatOutput(new NullOutput());
		InputOutputUtil.copy(input, output);
		assertEquals(output.getTotalRows(),rowsLimit);
	}
	
	@Test
	public void testRowsLimit() {
		int rowsLimit = 3;
		input.setRowsPerSecond(1);
		input.setRowsLimit(rowsLimit);
		StatOutput output = new StatOutput(new PrintOutput());
		InputOutputUtil.copy(input, output);
		assertEquals(output.getTotalRows(),rowsLimit);
	}
	
	@Test
	public void testRowsPerSecond() {
		int rowsPerSecond = 10;
		int rowsLimit = 100;

		input.setRowsPerSecond(rowsPerSecond);
		input.setRowsLimit(rowsLimit);
		StatOutput output = new StatOutput(new PrintOutput());
		
		long start = System.currentTimeMillis();
		InputOutputUtil.copy(input, output);
		
		long costSeconds = (System.currentTimeMillis() - start) / 1000;
		long totalSeconds = rowsLimit / rowsPerSecond;
		
		System.out.println("testRowsPerSecond() costSeconds:"+costSeconds+" totalSeconds:"+totalSeconds);
		assertTrue(costSeconds >= totalSeconds);
		assertTrue(costSeconds <= totalSeconds + 1);
	}
	
	@Test
	public void testIntervalSecond() {
		int intervalSecond = 3;
		int rowsPerSecond = 1;
		int rowsLimit = 5;

		input.setRowsPerSecond(rowsPerSecond);
		input.setRowsLimit(rowsLimit);
		input.setIntervalSecond(intervalSecond);
		StatOutput output = new StatOutput(new PrintOutput());
		
		long start = System.currentTimeMillis();
		InputOutputUtil.copy(input, output);
		
		
		long costSeconds = (System.currentTimeMillis() - start) / 1000;
		long totalSeconds = rowsLimit / rowsPerSecond * intervalSecond;
		System.out.println("testIntervalSecond() costSeconds:"+costSeconds+" totalSeconds:"+totalSeconds);
		assertTrue(costSeconds >= totalSeconds);
		assertTrue(costSeconds <= totalSeconds + 1);
	}

}
