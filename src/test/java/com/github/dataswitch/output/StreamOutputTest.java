package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.serializer.ByteDeserializer;
import com.github.dataswitch.serializer.ByteSerializer;

public class StreamOutputTest {

	ByteSerializer output = new ByteSerializer();
	@Test
	public void test() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		for(Object object : TestUtil.newTestDatas(10)) {
			output.serialize(object, baos);
		}
		
		byte[] buf = baos.toByteArray();
		ByteDeserializer input = new ByteDeserializer();
		int count = 0;
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		for(int i = 0; i < 100; i++) {
			Object obj = input.deserialize(bais);
			if(obj != null) {
				System.out.println(obj);
				count++;
			}
		}
		assertEquals(10,count);
	}

}
