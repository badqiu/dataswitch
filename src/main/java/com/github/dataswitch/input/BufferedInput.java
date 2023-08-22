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
	private List<Object> _bufferList;
	private boolean _reachEnd = false;
	
	public BufferedInput() {
		super();
	}

	public BufferedInput(Input proxy) {
		this(proxy,Constants.DEFAULT_BUFFER_SIZE);
	}
	
	public BufferedInput(Input proxy,int bufferSize) {
		super(proxy);
		this.bufferSize = bufferSize;
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
		
		
		List<Object> list = super.read(bufferSize);
		if(CollectionUtils.isEmpty(list)) {
			_reachEnd = true;
			return returnBufferList();
		}
		
		_bufferList.addAll(list);
		
		if(_bufferList.size() >= bufferSize) {
			return returnBufferList();
		}
		
		return read(size);
	}
	
	private List<Object> returnBufferList() {
		List result = _bufferList;
		_bufferList = new ArrayList(bufferSize);
		return result;
	}
	
	
}
