package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.IOUtil;
import com.github.dataswitch.util.InputOutputUtil;

public class ProxyOutput extends BaseObject implements Output{
	
	private Output proxy;
	
	private boolean autoFlush;

	public ProxyOutput() {
	}
	
	public ProxyOutput(Output proxy) {
		this(proxy,false);
	}
	
	public ProxyOutput(Output proxy,boolean autoFlush) {
		this.proxy = proxy;
		this.autoFlush = autoFlush;
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
	
	public boolean isAutoFlush() {
		return autoFlush;
	}

	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}

	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		proxy.write(rows);
		
		if(autoFlush) {
			flush();
		}
	}

	@Override
	public void close() throws Exception {
		InputOutputUtil.close(proxy);
	}
	
	@Override
	public void flush()  {
		InputOutputUtil.flush(proxy);
	}

	public void open(Map<String, Object> params) throws Exception {
		InputOutputUtil.open(params,proxy);
	}
	

}
