package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;

/**
 * 用synchronized同步的 Input,应用于多线程同步
 * 
 * @author badqiu
 *
 */
public class SyncInput extends ProxyInput{

	public SyncInput() {
		super();
	}

	public SyncInput(Input proxy) {
		super(proxy);
	}

	public synchronized List<Map<String, Object>> read(int size) {
		return super.read(size);
	}
	
}
