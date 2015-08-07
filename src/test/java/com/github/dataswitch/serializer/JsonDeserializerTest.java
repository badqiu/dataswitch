package com.github.dataswitch.serializer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;

import org.junit.Test;

public class JsonDeserializerTest {

	@Test
	public void test() throws Exception {
		JsonDeserializer jd = new JsonDeserializer();
		ByteArrayInputStream inputStream = new ByteArrayInputStream("{\"name\":\"badqiu\"},{\"name\":\"jane\"}".getBytes());
		Object obj = jd.deserialize(inputStream);
		assertEquals("{name=badqiu}",obj.toString());
		System.out.println(obj);
		obj = jd.deserialize(inputStream);
		assertEquals("{name=name}",obj.toString());
	}

}
