package com.github.dataswitch.input;

import org.junit.Test;

import com.github.dataswitch.util.InputOutputUtil;

public class FileBufferedInputTest {

	@Test
	public void test() {
		RandomStringInput input = new RandomStringInput(100);
		OutputStreamOutput output = new OutputStreamOutput(System.out);
		FileBufferedInput bufferedInput = new FileBufferedInput(input);
		bufferedInput.setDir("/tmp");
		bufferedInput.setFilename("FileBufferedInputTest.test");
		
		InputOutputUtil.copy(bufferedInput, output);
		InputOutputUtil.close(bufferedInput);
		InputOutputUtil.close(output);
	}

}
