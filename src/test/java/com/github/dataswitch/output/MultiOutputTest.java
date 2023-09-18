package com.github.dataswitch.output;

import java.io.PrintStream;

import org.junit.Test;

import com.github.dataswitch.input.DataGenInput;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.InputOutputUtil.CopyResult;

public class MultiOutputTest {

	@Test
	public void test() throws Exception {
		System.out.println("Runtime.getRuntime().availableProcessors()="+Runtime.getRuntime().availableProcessors());
		DataGenInput input = new DataGenInput(10);
		PrintStream sysout = new PrintStream(System.out);
		PrintOutput o1 = new PrintOutput(sysout,"o1");
		PrintOutput o2 = new PrintOutput(sysout,"o2");
		
		MultiOutput moutput = new MultiOutput(o1,o2);
		moutput.setConcurrent(true);
		moutput.setExecutorName("name1");
		moutput.open(null);
		
		CopyResult result = InputOutputUtil.copy(input, moutput);
		System.out.println(result);
		
		moutput.flush();
//		ExecutorServiceUtil.shutdownAllAndAwaitTermination();
		Thread.sleep(2000);
	}

}
