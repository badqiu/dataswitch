package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.github.dataswitch.input.QueueInput;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ThreadUtil;

public class QueueOutputTest {

	@Test
	public void test() {
		BlockingQueue queue = new ArrayBlockingQueue(100);
		QueueOutput output = new QueueOutput();
		output.setQueue(queue);
		QueueInput input = new QueueInput();
		input.setQueue(queue);
		
		int count = 20;
		for(int i = 0; i < count; i++) {
			output.write(Arrays.asList(i));
		}
		
		StatOutput statOutput = new StatOutput(new PrintOutput());
		Thread thread = new Thread(() -> {
			InputOutputUtil.copy(input, statOutput);
		});
		thread.start();
		
		ThreadUtil.sleep(2000);
		assertEquals(statOutput.getTotalRows(),count);
	}

}
