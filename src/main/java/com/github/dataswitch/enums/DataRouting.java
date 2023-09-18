package com.github.dataswitch.enums;

/**
 * 数据路由
 * 
 * @author badqiu
 *
 */
public enum DataRouting {
	ALL, //输出数据至所有output
	ROUND_ROBIN, //顺序选择一个output输出数据
	RANDOM; //随机选择一个output输出数据
//	HASH;
}
