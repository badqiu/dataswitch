package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;

public class LoggerOutput extends BaseObject implements Output {
	private String logger = LoggerOutput.class.getName();
	private String prefix = "";
	
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

	@Override
	public void close() throws IOException {
	}

	@Override
	public void write(List<Object> rows) {
		if(rows == null) return;
		if(_logger == null) {
			_logger = LoggerFactory.getLogger(logger);
		}
		
		for(Object row : rows) {
			_logger.info(prefix+row);
		}
		
	}
}
