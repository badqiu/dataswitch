package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.dataswitch.util.MapUtil;

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
		
		output.write(newMaps(5));
		
		Thread.sleep(3000);
		output.close();
	}

	private List<Map<String, Object>> newMaps(int size) {
		List<Map<String,Object>> r = new ArrayList<Map<String,Object>>();
		for(int i = 0; i < size; i++) {
			r.add(MapUtil.newMap("i",i));
		}
		return r;
	}

}
