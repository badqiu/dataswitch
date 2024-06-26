package com.github.dataswitch.input;

import org.junit.Test;

import com.github.dataswitch.util.InputOutputUtil;

public class BufferedInputTest {

	@Test
	public void test() throws Exception {
		RandomStringInput input = new RandomStringInput(20);
		
		OutputStreamOutput output = new OutputStreamOutput(System.out);
		BufferedInput bufferedInput = new BufferedInput(input,5,500);
		bufferedInput.open(null);
		
		InputOutputUtil.copy(bufferedInput, output,1);
		InputOutputUtil.close(bufferedInput);
		InputOutputUtil.close(output);
	}
	
}
