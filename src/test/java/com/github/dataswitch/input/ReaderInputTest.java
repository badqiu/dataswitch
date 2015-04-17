package com.github.dataswitch.input;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

import com.github.dataswitch.serializer.TxtDeserializer;

public class ReaderInputTest {

	TxtDeserializer input = new TxtDeserializer();
	@Test
	public void test() throws IOException {
		input.setColumns("username, age,sex");
		input.setColumnSplit(",");
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("badqiu,20,F\njane,15,M".getBytes());
		int count = 0;
		while(true) {
			Object obj = input.deserialize(byteArrayInputStream);
			if(obj == null) {
				break;
			}
			count++;
		}
		assertEquals(2,count);
	}

}
