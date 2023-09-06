package com.github.dataswitch.output;

import java.util.Arrays;

import org.junit.Test;

public class MultiFunctionOutputTest {

	@Test
	public void test() throws Exception {
		System.out.println("--------------- start ");

		MultiFunctionOutput output = new MultiFunctionOutput(new NullOutput());
		output.setPrint(true);
		output.setBatchSize(200);
		output.setBatchTimeout(1000);
		output.setStat(true);
		output.setSync(true);
		output.setAsync(true);
		output.setBuffered(true);
		output.setRetry(true);
		output.setRetryTimes(3);
		output.setLock(true);
		output.setLockId("hello_id");
		
		output.open(null);
		
		output.write(Arrays.asList(1,2,3,4,5));
		
		Thread.sleep(3000);
		output.close();
	}

}
