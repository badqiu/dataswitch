package com.github.dataswitch.enums;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum FailMode {
	FAIL_FAST("failFast","快速失败"),
	FAIL_AT_END("failAtEnd","结束时失败"),
	FAIL_NEVER("failNever","从不失败");
	
	
	
	private static Logger logger = LoggerFactory.getLogger(FailMode.class);
	
	private final String shortName;
	private final String desc;
	
	FailMode(String shortName,String desc) {
		this.shortName = shortName;
		this.desc = desc;
	}

	public String getShortName() {
		return shortName;
	}
	
	public String getDesc() {
		return desc;
	}
	
	public <T> void  forEach(Consumer<T> action,T... items) {
		forEach(action,Arrays.asList(items));
	}
	
	public <T> void  forEach(Consumer<T> action,List<T> items) {
		Exception lastException = null;
		T lastExceptionObject = null;
		
		for(T item : items) {
			try {
				action.accept(item);
			}catch(Exception e) {
				lastException = e;
				lastExceptionObject = item;
				
				if(this == FAIL_FAST) {
					throw new RuntimeException("failFast at:"+e+" on data:"+item,e);
				}
				
				logger.warn(this.name() + " at:"+e+" on data:"+item,e);
			}
		}
		
		if(this == FAIL_AT_END && lastException != null) {
			throw new RuntimeException("failAtEnd at:"+lastException+" on data:"+lastExceptionObject,lastException);
		}
		
	}

	public static FailMode getRequiredByName(String name) {
		if(StringUtils.isBlank(name)) {
			return null;
		}
		
		name = StringUtils.trim(name);
		
		for(FailMode m : values()) {
			if(m.getShortName().equalsIgnoreCase(name)) {
				return m;
			}
			if(m.name().equalsIgnoreCase(name)) {
				return m;
			}
		}
		throw new RuntimeException("not found failMode by name:"+name);
	}
}
