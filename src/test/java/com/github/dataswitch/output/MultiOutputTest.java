package com.github.dataswitch.output;

import org.junit.Test;

import com.github.dataswitch.input.DataGenInput;
import com.github.dataswitch.util.ExecutorServiceUtil;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.InputOutputUtil.CopyResult;

public class MultiOutputTest {

	@Test
	public void test() throws Exception {
		DataGenInput input = new DataGenInput(10);
		PrintOutput o1 = new PrintOutput(System.err,"o1");
		PrintOutput o2 = new PrintOutput(System.err,"o2");
		
		MultiOutput moutput = new MultiOutput(o1,o2);
		moutput.setConcurrent(true);
		moutput.setExecutorName("name1");
		moutput.open(null);
		
		CopyResult result = InputOutputUtil.copy(input, moutput);
		System.out.println(result);
		
//		ExecutorServiceUtil.shutdownAllAndAwaitTermination();
		Thread.sleep(3000);
	}

}
