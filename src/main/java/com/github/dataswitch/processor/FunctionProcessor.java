package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * function的代理
 * @author badqiu
 *
 */
public class FunctionProcessor implements Processor {
	
	private Function function;
	private boolean processOne = true;
	
	
	public FunctionProcessor() {
	}
	
	public FunctionProcessor(Function function) {
		super();
		this.function = function;
	}

	
	public FunctionProcessor(Function function, boolean processOne) {
		super();
		this.function = function;
		this.processOne = processOne;
	}

	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}

	public boolean isProcessOne() {
		return processOne;
	}

	public void setProcessOne(boolean processOne) {
		this.processOne = processOne;
	}

	@Override
	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception {
		if(processOne) {
			List<Map<String, Object>> results = new ArrayList(datas.size());
			for(Map<String,Object> object : datas) {
				Map<String,Object> result = (Map)function.apply(object);
				if(result != null) {
					results.add(result);
				}
			}
			return results;
		}else {
			return (List)function.apply(datas);
		}
	}

}
