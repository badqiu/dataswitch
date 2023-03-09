package com.github.dataswitch.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * 数据类型枚举
 * 
 * @author badqiu
 *
 */
public enum DataType {
	STRING,CHAR,
	
	LONG,INTEGER,INT,BIGINTEGER,SHORT,
	FLOAT,DOUBLE,BIGDECIMAL,
	
	BOOLEAN,
	
	TIMESTAMP,DATE,TIME;
	
	
	public boolean isDouble() {
		if(this == FLOAT || this == DOUBLE || this == BIGDECIMAL) {
			return true;
		}
		return false;
	}
	
	public boolean isInteger() {
		if(this == LONG || this == INTEGER || this == INT || this == BIGINTEGER || this == SHORT) {
			return true;
		}
		return false;
	}
	
	public boolean isDate() {
		if(this == TIMESTAMP || this == DATE || this == TIME ) {
			return true;
		}
		return false;
	}
	
	public boolean isString() {
		if(this == STRING || this == CHAR) {
			return true;
		}
		return false;
	}
	
	public boolean eq(String other) {
		if(other == null) return false;
		
		String upper = other.toUpperCase();
		return upper.equals(name());
	}
	
	public static DataType getByName(String name) {
		if(StringUtils.isBlank(name)) return null;
		
		String uppperCase = name.toUpperCase();
		return valueOf(uppperCase);
		
//		for(DataType item : values()) {
//			if(item.eq(name)) {
//				return item;
//			}
//			if(item.name().equals(name)) {
//				return item;
//			}
//		}
//		
//		return null;
	}
	
}
