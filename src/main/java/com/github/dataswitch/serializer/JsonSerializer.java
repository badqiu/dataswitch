package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.BaseObject;

public class JsonSerializer  extends BaseObject implements Serializer<Map>{

	static ObjectMapper objectMapper = new ObjectMapper();
	@Override
	public void serialize(Map object, OutputStream outputStream)
			throws IOException {
		if(object == null) return;
		
		byte[] bytes = objectMapper.writeValueAsBytes(object);
//		objectMapper.writeValue(outputStream, object);
		outputStream.write(bytes);
		outputStream.write("\n".getBytes());
	}

	@Override
	public void flush() throws IOException {
	}

}
