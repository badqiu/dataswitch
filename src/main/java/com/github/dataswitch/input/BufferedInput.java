package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;

public class BufferedInput extends ProxyInput{
	private static Logger logger = LoggerFactory.getLogger(BufferedInput.class);
	
	private int bufferSize = Constants.DEFAULT_BUFFER_SIZE;
	private int bufferTimeout = 0;
	
	
	private long _lastFlushTime = System.currentTimeMillis();
	private List<Object> _bufferList;
	private boolean _reachEnd = false;
	
	public BufferedInput() {
		super();
	}

	public BufferedInput(Input proxy) {
		this(proxy,Constants.DEFAULT_BUFFER_SIZE,0);
	}
	
	public BufferedInput(Input proxy,int bufferSize,int bufferTimeout) {
		super(proxy);
		
		this.bufferSize = bufferSize;
		this.bufferTimeout = bufferTimeout;
	}
	
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getBufferTimeout() {
		return bufferTimeout;
	}

	public void setBufferTimeout(int bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		if(_bufferList == null) {
			_bufferList = new ArrayList(bufferSize);
		}
	}

	@Override
	public List<Object> read(int size) {
		if(_reachEnd) {
			return Collections.EMPTY_LIST;
		}
		
		
		List<Object> list = super.read(size);
		if(CollectionUtils.isEmpty(list)) {
			_reachEnd = true;
			return returnBufferList();
		}
		
		_bufferList.addAll(list);
		
		if(_bufferList.size() >= bufferSize) {
			return returnBufferList();
		}
		if(isTimeout()) {
			return returnBufferList();
		}
		
		return read(size);
	}
	
	private boolean isTimeout() {
		if(bufferTimeout <= 0) {
			return false;
		}
		
		long interval = System.currentTimeMillis() - _lastFlushTime;
		return interval > bufferTimeout;
	}

	private List<Object> returnBufferList() {
		List result = _bufferList;
		_bufferList = new ArrayList(bufferSize);
		
		if(bufferTimeout > 0) {
			_lastFlushTime = System.currentTimeMillis();
		}
		
		return result;
	}
	
	
}
