package com.github.dataswitch.enums;

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
		if(this == STRING) {
			return true;
		}
		return false;
	}
}
