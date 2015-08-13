package com.github.dataswitch.serializer;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import com.github.rapid.common.util.MapUtil;

public class JsonSerializerTest {

	@Test
	public void test() throws Exception {
		JsonSerializer jser = new JsonSerializer();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		jser.serialize(MapUtil.newLinkedMap("name","badqiu","age",20), baos);
		jser.flush();
		assertEquals("{\"name\":\"badqiu\",\"age\":20}",baos.toString());
		
		
		jser.serialize(null, baos);
		assertEquals("{\"name\":\"badqiu\",\"age\":20}",baos.toString());
	}

}