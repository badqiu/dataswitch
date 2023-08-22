package com.github.dataswitch.input;

import org.junit.Test;

import com.github.dataswitch.util.InputOutputUtil;

public class FileBufferedInputTest {

	@Test
	public void test() {
		RandomStringInput input = new RandomStringInput(10);
		
		OutputStreamOutput output = new OutputStreamOutput(System.out);
		FileBufferedInput bufferedInput = new FileBufferedInput(input);
		bufferedInput.setDir("/tmp");
		bufferedInput.setFilename("FileBufferedInputTest.test");
		bufferedInput.setDeleteFileOnClose(true);
		
		InputOutputUtil.copy(bufferedInput, output,1);
		InputOutputUtil.close(bufferedInput);
		InputOutputUtil.close(output);
	}

}
