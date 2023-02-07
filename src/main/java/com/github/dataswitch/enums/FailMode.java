package com.github.dataswitch.enums;

import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.Util;

public enum FailMode {
	//RETRY ??
	//FAIL_BY_EXCEPTION ??
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
	
	/**
	 * 
	 * @param <T>
	 * @param items
	 * @param action
	 * @return 最后的异常，如果是FailNever
	 */
	public <T> Exception  forEach(T[] items,Consumer<T> action) {
		if(items == null) return null;
		
		return forEach(Arrays.asList(items),action);
	}
	
	/**
	 * 
	 * @param <T>
	 * @param items
	 * @param action
	 * @return 最后的异常，如果是FailNever
	 */	
	public <T> Exception  forEach(Iterable<T> items,Consumer<T> action) {
		if(items == null) return null;
		
		if(this == FAIL_FAST) { 
			for(T item : items) {
				action.accept(item);
			}
			return null;
		}
		
		
		Exception lastException = null;
		T lastExceptionData = null;
		int totalErrorCount = 0;
		
		for(T item : items) {
			try {
				action.accept(item);
			}catch(Exception e) {
				totalErrorCount++;
				lastException = e;
				lastExceptionData = item;
				
				Object errorData = Util.first(Util.first(item));
				if(this == FAIL_FAST) {
					throw new RuntimeException("failFast on data first row:"+errorData,e);
				}
				logger.warn(this.name() + " at:"+e+" on data:"+errorData,e);
			}
		}
		
		throwExceptionIfFailAtEnd(lastException,lastExceptionData,totalErrorCount);
		
		return lastException;
	}
	
	public void handleException(Exception e,String exceptionMessage) {
		if(this == FAIL_FAST) {
			if(exceptionMessage == null) {
				throw new RuntimeException(e);
			}
			throw new RuntimeException(exceptionMessage,e);
		}
		
		if(exceptionMessage == null) {
			logger.warn("has error",e);
		}else {
			logger.warn(exceptionMessage,e);
		}
	}
	
	public void throwExceptionIfFailAtEnd(Exception lastException,Object lastExceptionData) {
		throwExceptionIfFailAtEnd(lastException,lastExceptionData,1);
	}
	
	public void throwExceptionIfFailAtEnd(Exception lastException,Object lastExceptionData,int totalErrorCount) {
		if(this == FAIL_AT_END && lastException != null) {
			Object lastData = Util.first(Util.first(lastExceptionData));
			String message = this.name() +" with lastException on lastData:"+lastData + " totalErrorCount:" + totalErrorCount+" ";
			throw new RuntimeException(message,lastException);
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
