package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;

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

	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		proxy.write(rows);
	}

	@Override
	public void close() {
		IOUtil.closeQuietly(proxy);
	}
	
	@Override
	public void flush() throws IOException {
		proxy.flush();
	}

}
