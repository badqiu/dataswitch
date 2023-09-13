package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.IOUtil;

public class ProxyOutput extends BaseObject implements Output{
	
	private Output proxy;

	public ProxyOutput() {
	}
	
	public ProxyOutput(Output proxy) {
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
	
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		proxy.write(rows);
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
	}
	

}
