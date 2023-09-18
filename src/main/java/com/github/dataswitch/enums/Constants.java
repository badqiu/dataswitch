package com.github.dataswitch.enums;

public class Constants {

	/**
	 * 默认缓存条数
	 */
	public static int DEFAULT_BATCH_SIZE = 1000;
	
	/**
	 * 默认缓存超时时间(毫秒)
	 */
	public static int DEFAULT_BATCH_TIMEOUT = 500; 
	
	/**
	 * lock的默认分组
	 */
	public static String DEFAULT_LOCK_GROUP = System.getProperty("DEFAULT_LOCK_GROUP","default");
	
	/**
	 * queue的默认分组
	 */
	public static String DEFAULT_QUEUE_GROUP = System.getProperty("DEFAULT_QUEUE_GROUP","default");

	/**
	 * queue的默认分组
	 */
	public static String DEFAULT_EXECUTOR_GROUP = System.getProperty("DEFAULT_QUEUE_GROUP","default");

	
	/**
	 * 线程池大小
	 */
	public static int EXECUTOR_SERVICE_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	
	/**
	 * 线程池在系统退出时的等待时间
	 */
	public static int EXECUTOR_SERVICE_AWAIT_TERMINATION_SECOND = 30;
	
	/**
	 * hive列分隔符
	 **/
	public static final String COLUMN_SPLIT = "\001";
	
	/**
	 * hive 的null 值特殊转义字符
	 */
	public static final String NULL_VALUE = "\\N";

}
