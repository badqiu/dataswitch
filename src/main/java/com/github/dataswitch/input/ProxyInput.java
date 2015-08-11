package com.github.dataswitch.input;

import java.util.List;

import com.github.dataswitch.util.IOUtil;

public class ProxyInput implements Input{

	private Input proxy;

	public ProxyInput() {
	}
	
	public ProxyInput(Input proxy) {
		super();
		this.proxy = proxy;
	}

	public List<Object> read(int size) {
		return proxy.read(size);
	}

	public void close() {
		IOUtil.closeQuietly(proxy);
	}

	public Input getProxy() {
		return proxy;
	}

	public void setProxy(Input proxy) {
		this.proxy = proxy;
	}

}
