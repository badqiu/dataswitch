package com.github.dataswitch.runner.param;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.util.InputOutputUtil;

public class InputParameterGenerator implements Supplier<Map<String,Object>>{

	private Input input;
	
	private String key;
	private String value;
	private String defaultValue;

	public Input getInput() {
		return input;
	}

	public void setInput(Input input) {
		this.input = input;
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public Map<String, Object> get() {
		List<Map> rows = (List)InputOutputUtil.readFully(input, 500);
		return buildOneMap(rows,key,value,defaultValue);
	}

	public static Map<String, Object> buildOneMap(List<Map> rows,String key,String value,String defaultValue) {
		Map map = new HashMap();
		for(Map row : rows) {
			Object k = row.get(key);
			Object v = row.get(value);
			
			if(isBlank(v) && StringUtils.isNotBlank(defaultValue)) {
				v = row.get(defaultValue);
			}
			
			if(k != null) {
				row.put(k, v);
			}
		}
		return map;
	}

	private static boolean isBlank(Object v) {
		return v == null || StringUtils.isBlank(Objects.toString(v,""));
	}
	
	
}
