package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonProcessor implements Processor{

	private ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		List result = new ArrayList();
		for(Object object : datas) {
			String str = objectMapper.writeValueAsString(object);
			result.add(str);
		}
		return result;
	}

}