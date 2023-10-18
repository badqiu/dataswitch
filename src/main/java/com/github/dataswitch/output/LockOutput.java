package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.support.LockProvider;

public class LockOutput extends LockProvider implements Output {

	private Output proxy;
	
	public LockOutput() {
	}
	
	public LockOutput(Output proxy) {
		super();
		this.proxy = proxy;
	}

	public Output getProxy() {
		return proxy;
	}

	public void setProxy(Output proxy) {
		this.proxy = proxy;
	}

	public void setTarget(Output proxy) {
		setProxy(proxy);
	}
	
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		Lock lock = getLock();
		try {
			lock.lock();
			proxy.write(rows);
		}finally {
			lock.unlock();
		}
	}

	@Override
	public void close() throws Exception {
		proxy.close();
	}
	
	@Override
	public void flush() throws IOException {
		proxy.flush();
	}

	public void open(Map<String, Object> params) throws Exception {
		proxy.open(params);
		super.open(params);
	}
	
}
