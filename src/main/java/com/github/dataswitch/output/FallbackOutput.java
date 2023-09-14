package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.InputOutputUtil;


public class FallbackOutput extends ProxyOutput{

	private static Logger logger = LoggerFactory.getLogger(FallbackOutput.class);
	private Output fallback;

	public FallbackOutput() {
		super();
	}

	public FallbackOutput(Output proxy) {
		super(proxy);
	}
	
	public FallbackOutput(Output proxy,Output fallback) {
		super(proxy);
		setFallback(fallback);
	}

	public Output getFallback() {
		return fallback;
	}

	public void setFallback(Output fallback) {
		this.fallback = fallback;
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		try {
			super.write(rows);
		}catch(Exception e) {
			logger.warn("fallback write rows,fallback output:"+fallback+" rows.size:"+rows.size());
			fallback.write(rows);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		InputOutputUtil.open(params, fallback);
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		InputOutputUtil.close(fallback);
	}
	
}
