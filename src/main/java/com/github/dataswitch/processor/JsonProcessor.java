package com.github.dataswitch.processor;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * json序列化processor
 * 
 * @author badqiu
 *
 */
public class JsonProcessor extends BaseProcessor implements Processor{

	private ObjectMapper objectMapper = new ObjectMapper();
	
	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	protected Map<String, Object> processOne(Map<String,Object> row) throws Exception {
		String str = objectMapper.writeValueAsString(row);
		Map map = new HashMap();
		map.put("json", str);
		return map;
	}

}