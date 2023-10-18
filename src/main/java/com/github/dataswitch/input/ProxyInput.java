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
		setProxy(proxy);
	}

	public Input getProxy() {
		return proxy;
	}

	public void setProxy(Input proxy) {
		this.proxy = proxy;
	}
	
	public void setTarget(Input proxy) {
		setProxy(proxy);
	}
	
	@Override
	public void commitInput() {
		this.proxy.commitInput();
	}

	public List<Map<String, Object>> read(int size) {
		return proxy.read(size);
	}

	public void close() throws Exception {
		InputOutputUtil.close(proxy);
	}
	
	public void open(Map<String, Object> params) throws Exception {
		InputOutputUtil.open(params,proxy);
	}

}
