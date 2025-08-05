package com.github.dataswitch.util;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

	static ObjectMapper objectMapper = new ObjectMapper();

	public static String toJsonString(Object obj) {
		try {
			return objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static Object parseJson(String string) {
		if(StringUtils.isBlank(string)) {
			return null;
		}
		
		try {
			return objectMapper.readValue(string, Object.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
