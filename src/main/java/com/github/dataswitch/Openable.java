package com.github.dataswitch;

import java.util.Map;

/** 开始执行之前 */
public interface Openable {

	public void open(Map<String,Object> params) throws Exception;
	
}
