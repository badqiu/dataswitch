package com.github.dataswitch.enums;
/** 
 * JdbcOutput 的输出模式 
 * */
public enum OutputMode {
	delete,
	insert,
	upsert, //insert or update
	update
}