package com.github.dataswitch.serializer;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.junit.Test;

import com.github.dataswitch.input.FileInput;
import com.github.dataswitch.output.FileOutput;
import com.github.dataswitch.util.MapUtil;

public class JsonSerializerTest {

	@Test
	public void test() throws Exception {
		JsonSerializer jser = new JsonSerializer();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		jser.serialize(MapUtil.newLinkedMap("name","badqiu1","age",20), baos);
		jser.serialize(MapUtil.newLinkedMap("name","badqiu2","age",22), baos);
		jser.flush();
		assertEquals("{\"name\":\"badqiu1\",\"age\":20}\n{\"name\":\"badqiu2\",\"age\":22}\n",baos.toString());
		
		
		jser.serialize(null, baos);
		assertEquals("{\"name\":\"badqiu1\",\"age\":20}\n{\"name\":\"badqiu2\",\"age\":22}\n",baos.toString());
	}
	
	@Test
	public void test_with_fileoutput() {
		FileOutput output = new FileOutput();
		output.setDir("/tmp/JsonSerializerTest/");
		output.setFilename("JsonSerializerTest.json");
		output.setSerializer(new JsonSerializer());
		output.writeObject(MapUtil.newLinkedMap("name","badqiu1","age",20));
		output.writeObject(MapUtil.newLinkedMap("name","badqiu2","age",22));
		output.close();
		
		FileInput input = new FileInput();
		input.setDeserializer(new JsonDeserializer());
		input.setDir("/tmp/JsonSerializerTest/");
		
		List<Object> rows = input.read(100);
		System.out.println(rows.toString());
		assertEquals("[{name=badqiu1, age=20}, {name=badqiu2, age=22}]",rows.toString());
	}

}
