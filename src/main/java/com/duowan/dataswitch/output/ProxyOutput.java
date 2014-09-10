package com.duowan.dataswitch.output;

import java.util.List;

import com.duowan.dataswitch.util.IOUtil;

public class ProxyOutput implements Output{
	
	private Output proxy;

	public ProxyOutput(Output proxy) {
		super();
		this.proxy = proxy;
	}

	public void write(List<Object> rows) {
		proxy.write(rows);
	}

	public void close() {
		IOUtil.closeQuietly(proxy);
	}
	
	

}
