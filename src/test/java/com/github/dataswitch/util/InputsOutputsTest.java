package com.github.dataswitch.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;

public class InputsOutputsTest {

	@Test()
	public void test_new_and_exec() {
		InputsOutputs job = new InputsOutputs();
		job.exec();
	}
	
	int count = 0;
	int writeCount = 0;
	@Test
	public void test_async() {
		InputsOutputs job = new InputsOutputs();
		job.setAsync(true);
		
		job.setInput(new Input() {
			@Override
			public void close() throws IOException {
			}
			
			@Override
			public List read(int size) {
				count++;
				if(count >= 100) return null;
				return Arrays.asList(count);
			}
		});
		
		job.setOutput(new Output() {
			@Override
			public void close() throws IOException {
			}
			
			@Override
			public void write(List<Object> rows) {
				for(Object row : rows) {
					System.out.println(row);
					writeCount++;
				}
			}
		});
		
		job.exec();
		
		assertEquals(99,writeCount);
		
		
		count = 0;
		job.setSync(true);
		job.exec();
		assertEquals(198,writeCount);
	}

}
