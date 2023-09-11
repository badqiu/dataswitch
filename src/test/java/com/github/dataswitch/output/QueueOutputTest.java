package com.github.dataswitch.output;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.github.dataswitch.input.QueueInput;
import com.github.dataswitch.util.InputOutputUtil;

public class QueueOutputTest {

	@Test
	public void test() {
		BlockingQueue queue = new ArrayBlockingQueue(100);
		QueueOutput output = new QueueOutput();
		output.setQueue(queue);
		QueueInput input = new QueueInput();
		input.setQueue(queue);
		
		for(int i = 0; i < 20; i++) {
			output.write(Arrays.asList(i));
		}
		
		InputOutputUtil.copy(input, new PrintOutput());
		
		
	}

}
