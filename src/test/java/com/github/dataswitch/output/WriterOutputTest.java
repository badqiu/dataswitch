package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.serializer.TxtSerializer;

public class WriterOutputTest {

	TxtSerializer output = new  TxtSerializer();
	@Test
	public void test() throws IOException {
		List<Object> rows = TestUtil.newTestDatas(2);
		output.setColumns("pwd, age,username");
		output.setColumnSeparator(",");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for(Object obj : rows) {
			output.serialize(obj, out);
		}
		output.flush();
		String string = out.toString();
		System.out.println(string);
		assertEquals(string,"123,20,badqiu_0\n123,21,badqiu_1\n");
	}

}
