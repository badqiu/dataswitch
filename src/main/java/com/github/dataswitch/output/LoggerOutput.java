package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;

public class LoggerOutput extends BaseObject implements Output {
	private String logger = LoggerOutput.class.getName();
	
	private String prefix = null;
	
	private Logger _logger = null;
	
	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}
	
	public void setLogger(Class logger) {
		this.logger = logger.getName();
	}

	@Override
	public void write(List<Object> rows) {
		if(rows == null) return;
		
		for(Object row : rows) {
			if(prefix == null) {
				_logger.info(String.valueOf(row));
			}else {
				_logger.info(prefix + row);
			}
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output.super.open(params);
		init();
	}

	private void init() {
		if(_logger == null) {
			_logger = LoggerFactory.getLogger(logger);
		}
	}
}
