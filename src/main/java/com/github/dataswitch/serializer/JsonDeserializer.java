package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.serializer.Deserializer;

import com.github.dataswitch.BaseObject;

public class JsonDeserializer  extends BaseObject implements Deserializer<Map>{

	static ObjectMapper objectMapper = new ObjectMapper();
	private Class valueType = Map.class;
	
	@Override
	public Map deserialize(InputStream inputStream) throws IOException {
		return objectMapper.readValue(inputStream, valueType);
	}

}
