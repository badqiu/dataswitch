package com.github.dataswitch.processor;

import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
/**
 * 默认处理器，不做任何操作
 * 
 * @author badqiu
 *
 */
public class DefaultProcessor implements Processor {

	public List<Map<String, Object>> process(List<Map<String, Object>> datas) {
		return datas;
	}

}
