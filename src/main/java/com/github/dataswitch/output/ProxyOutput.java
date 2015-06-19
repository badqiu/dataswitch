package com.github.dataswitch.output;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.util.IOUtil;

public class ProxyOutput implements Output{
	
	private Output proxy;

	public ProxyOutput(Output proxy) {
		super();
		this.proxy = proxy;
	}

	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		proxy.write(rows);
	}

	public void close() {
		IOUtil.closeQuietly(proxy);
	}
	
	

}
