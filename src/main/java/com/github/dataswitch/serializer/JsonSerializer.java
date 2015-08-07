package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.github.dataswitch.BaseObject;

public class JsonSerializer  extends BaseObject implements Serializer<Map>{

	static ObjectMapper objectMapper = new ObjectMapper();
	@Override
	public void serialize(Map object, OutputStream outputStream)
			throws IOException {
		if(object == null) return;
		
		objectMapper.writeValue(outputStream, object);
	}

	@Override
	public void flush() throws IOException {
	}

}
