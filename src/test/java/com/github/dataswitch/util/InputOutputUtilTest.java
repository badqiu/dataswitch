package com.github.dataswitch.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.dataswitch.input.CollectDataOutput;
import com.github.dataswitch.input.RandomStringInput;
import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;
import com.github.dataswitch.processor.ScriptProcessor;
import com.github.dataswitch.processor.SqlProcessor;

public class InputOutputUtilTest {
	@Test
	public void test() {
		CollectDataOutput output = new CollectDataOutput();
		InputOutputUtil.copy(new RandomStringInput(10),output, null);
		assertEquals(output.getDatas().toString(),"[{num=0}, {num=1}, {num=2}, {num=3}, {num=4}, {num=5}, {num=6}, {num=7}, {num=8}, {num=9}]");
		
		output = new CollectDataOutput();
		SqlProcessor sqlProcessor = new SqlProcessor("select * from t where num >= 7");
		InputOutputUtil.copy(new RandomStringInput(10),output, sqlProcessor);
		assertEquals(output.getDatas().toString(),"[{num=7}, {num=8}, {num=9}]");
		
		output = new CollectDataOutput();
		ScriptProcessor scriptProcessor = new ScriptProcessor("groovy","row.put('name','groovy');return row;");
		InputOutputUtil.copy(new RandomStringInput(5),output, scriptProcessor);
		assertEquals(output.getDatas().toString(),"[{num=0, name=groovy}, {num=1, name=groovy}, {num=2, name=groovy}, {num=3, name=groovy}, {num=4, name=groovy}]");
		
		output = new CollectDataOutput();
		Processor multiProcessor = new MultiProcessor(sqlProcessor,scriptProcessor);
		InputOutputUtil.copy(new RandomStringInput(10),output, multiProcessor);
		assertEquals(output.getDatas().toString(),"[{num=7, name=groovy}, {num=8, name=groovy}, {num=9, name=groovy}]");
		
	}

}
