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
	
	private int batchSize = Constants.DEFAULT_BATCH_SIZE;
	private int batchTimeout = 0;
	
	
	private long _lastFlushTime = System.currentTimeMillis();
	private List<Object> _bufferList;
	private boolean _reachEnd = false;
	
	public BufferedInput() {
		super();
	}

	public BufferedInput(Input proxy) {
		this(proxy,Constants.DEFAULT_BATCH_SIZE,0);
	}
	
	public BufferedInput(Input proxy,int batchSize,int batchTimeout) {
		super(proxy);
		
		this.batchSize = batchSize;
		this.batchTimeout = batchTimeout;
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		if(_bufferList == null) {
			_bufferList = new ArrayList(batchSize);
		}
	}

	@Override
	public List<Map<String, Object>> read(int size) {
		if(_reachEnd) {
			return Collections.EMPTY_LIST;
		}
		
		
		List<Map<String, Object>> list = super.read(size);
		if(CollectionUtils.isEmpty(list)) {
			_reachEnd = true;
			return returnBufferList();
		}
		
		_bufferList.addAll(list);
		
		if(_bufferList.size() >= batchSize) {
			return returnBufferList();
		}
		if(isTimeout()) {
			return returnBufferList();
		}
		
		return read(size);
	}
	
	private boolean isTimeout() {
		if(batchTimeout <= 0) {
			return false;
		}
		
		long interval = System.currentTimeMillis() - _lastFlushTime;
		return interval > batchTimeout;
	}

	private List<Map<String, Object>> returnBufferList() {
		List result = _bufferList;
		_bufferList = new ArrayList(batchSize);
		
		if(batchTimeout > 0) {
			_lastFlushTime = System.currentTimeMillis();
		}
		
		return result;
	}
	
	
}
