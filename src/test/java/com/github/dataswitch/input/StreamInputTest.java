package com.github.dataswitch.input;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.serializer.ByteDeserializer;
import com.github.dataswitch.util.IOUtil;

public class StreamInputTest {

	ByteDeserializer input = new ByteDeserializer();
	@Test
	public void test() throws Exception {
		byte[] buf = newTestBuf(10);
		int count = 0;
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		for(int i = 0; i < 100; i++) {
			Object obj = input.deserialize(bais);
			if(obj != null) {
				count++;
				System.out.println(obj);
			}
		}
		assertEquals(count,100);
	}
	
	private byte[] newTestBuf(int size) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		for(int i = 0; i < size; i++) {
			byte[] buf = IOUtil.javaObject2Bytes(TestUtil.newTestDatas(size));
			IOUtil.writeWithLength(new DataOutputStream(baos), buf);
		}
		return baos.toByteArray();
	}

}
