package com.github.dataswitch.enums;

public class Constants {

	/**
	 * 默认缓存条数
	 */
	public static int DEFAULT_BUFFER_SIZE = 1000;
	
	/**
	 * 默认缓存超时时间(毫秒)
	 */
	public static int DEFAULT_BUFFER_TIMEOUT = 500; 
	
	/**
	 * lock的默认分组
	 */
	public static String DEFAULT_LOCK_GROUP = System.getProperty("DEFAULT_LOCK_GROUP","default");
	
	/**
	 * MultiOutput,MultiInput并发执行时，close时默认的等待时间
	 */
	public static int ON_CLOSE_EXECUTOR_AWAIT_TERMINATION_SECOND = 10;
	
	/**
	 * hive列分隔符
	 **/
	public static final String COLUMN_SPLIT = "\001";
	
	/**
	 * hive 的null 值特殊转义字符
	 */
	public static final String NULL_VALUE = "\\N";

	
	
}
