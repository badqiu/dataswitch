package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;

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
	protected Object processOne(Object row) throws Exception {
		String str = objectMapper.writeValueAsString(row);
		return str;
	}

}