package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.InputOutputUtil;

public class ProxyInput extends BaseObject  implements Input{

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

	public void close() throws Exception {
		InputOutputUtil.closeQuietly(proxy);
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

}
