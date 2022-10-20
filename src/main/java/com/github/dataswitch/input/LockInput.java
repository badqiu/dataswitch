package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.github.dataswitch.support.LockProvider;
import com.github.dataswitch.util.IOUtil;

public class LockInput extends LockProvider implements Input {

	
	private Input proxy;

	public LockInput() {
	}
	
	public LockInput(Input proxy) {
		this.proxy = proxy;
	}

	public void close() throws Exception {
		if(proxy != null) {
			proxy.close();
		}
	}

	public Input getProxy() {
		return proxy;
	}

	public void setProxy(Input proxy) {
		this.proxy = proxy;
	}
	
	@Override
	public void commitInput() {
		this.proxy.commitInput();
	}

	public void open(Map<String, Object> params) throws Exception {
		proxy.open(params);
	}
	
	@Override
	public List<Object> read(int size) {
		Lock lock = getLock();
		try {
			lock.lock();
			return proxy.read(size);
		}finally {
			lock.unlock();
		}
	}
	
}
