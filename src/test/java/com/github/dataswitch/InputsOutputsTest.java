package com.github.dataswitch;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.aspectj.lang.annotation.After;
import org.junit.Test;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.util.MapUtil;
import com.github.dataswitch.util.ThreadUtil;

public class InputsOutputsTest {

	InputsOutputs job = new InputsOutputs();

	@Test()
	public void test_new_and_exec() {
		job.exec();
	}
	
	@Test(expected = RuntimeException.class)
	public void test_setErrorOnNoData() {
		job.setErrorOnNoData(true);
		job.exec();
	}
	
	volatile int count = 0;
	volatile int writeCount = 0;
	@Test
	public void test_async() {
		job.setAsync(true);
		
		initInputOutput();
		
		job.exec();
		
		assertEquals(100,count);
		assertEquals(99,writeCount);
		
	}

	@Test
	public void testSyncWithBatch() {
		initInputOutput();
		job.setSync(true);
		job.setBatchSize(50);
		job.exec();
		assertEquals(100,count);
		assertEquals(99,writeCount);
	}

	@Test
	public void testSync() {
		initInputOutput();
		job.setSync(true);
		job.exec();
		assertEquals(100,count);
		assertEquals(99,writeCount);
	}

	private void initInputOutput() {
		job.setInput(new Input() {
			@Override
			public List<Map<String, Object>> read(int size) {
				count++;
				ThreadUtil.sleep(10);
				if(count >= 100) return null;
				
				Map map = MapUtil.newMap("count",0 + count);
				return Arrays.asList(map);
			}
		});
		
		job.setOutput(new Output() {
			@Override
			public void write(List<Map<String, Object>> rows) {
				for(Object row : rows) {
//					System.out.println(row+" writeCount:"+writeCount+" count:"+count+" rows.size:"+rows.size());
					writeCount++;
				}
				System.out.println(" -------- rows.size:"+rows.size());
			}
		});
	}
	
	@org.junit.After
	public void after() throws Exception {
		job.close();
		Thread.sleep(1000 * 3);
	}

}
