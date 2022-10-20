package com.github.dataswitch.enums;

public class Constants {

	public static int DEFAULT_BUFFER_SIZE = 1000;

	public static String DEFAULT_LOCK_GROUP = System.getProperty("DEFAULT_LOCK_GROUP","default");
	
	/**
	 * hive列分隔符
	 **/
	public static final String COLUMN_SPLIT = "\001";
	
	/**
	 * hive 的null 值特殊转义字符
	 */
	public static final String NULL_VALUE = "\\N";
	
}
