package com.github.dataswitch.serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.serializer.Deserializer;

import com.github.dataswitch.BaseObject;

public class JsonDeserializer  extends BaseObject implements Deserializer<Map>{

	static ObjectMapper objectMapper = new ObjectMapper();
	private Class valueType = Map.class;
	
	private Map<InputStream,BufferedReader> readers = new WeakHashMap<InputStream,BufferedReader>();
	
	@Override
	public Map deserialize(InputStream inputStream) throws IOException {
		BufferedReader reader = toBufferedReader(inputStream);
		String line = null;
		while((line = reader.readLine()) != null) {
			if(StringUtils.isBlank(line)) {
				continue;
			}
			return (Map)objectMapper.readValue(line, valueType);
		}
		return null;
//		return readByJsonNode(inputStream);
//		return objectMapper.readValue(inputStream, valueType);
	}

	private BufferedReader toBufferedReader(InputStream inputStream) {
		BufferedReader reader = readers.get(inputStream);
		if(reader == null) {
			reader = new BufferedReader(new InputStreamReader(inputStream));
			readers.put(inputStream, reader);
		}
		return reader;
	}
	
//	private Map<InputStream,JsonNode> map = new HashMap<InputStream,JsonNode>();
//	private Map readByJsonNode(InputStream inputStream) throws IOException,
//			JsonProcessingException, JsonParseException, JsonMappingException {
//		JsonNode jsonNode = map.get(inputStream);
//		if(jsonNode == null) {
//			jsonNode = objectMapper.readTree(inputStream);
//			map.put(inputStream, jsonNode);
//		}
//		return objectMapper.readValue(jsonNode, valueType);
//	}

}
