package com.github.dataswitch.input;

import java.util.List;

import com.github.dataswitch.util.IOUtil;

public class ProxyInput implements Input{

	private Input proxy;

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

}
