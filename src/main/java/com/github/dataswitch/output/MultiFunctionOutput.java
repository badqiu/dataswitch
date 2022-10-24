package com.github.dataswitch.output;

import java.util.Map;

public class MultiFunctionOutput extends ProxyOutput {

	private boolean async = false;
	private boolean sync = false;
	private boolean lock = false;
	private boolean buffered = false;
	private boolean retry = false;
	
	public MultiFunctionOutput() {
		super();
	}

	public MultiFunctionOutput(Output proxy) {
		setProxy(proxy);
	}
	
	@Override
	public void setProxy(Output proxy) {
		super.setProxy(proxy);
	}
	
	private Output newProxy(Output proxy) {
		Output output = proxy;
		if(sync) {
			output = new SyncOutput(output);
		}
		if(buffered) {
			output = new BufferedOutput(output);
		}
		if(retry) {
			output = new RetryOutput(output);
		}
		if(async) {
			output = new AsyncOutput(output);
		}
		if(lock) {
			output = new LockOutput(output);
		}
		return output;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		Output newProxy = newProxy(getProxy());
		setProxy(newProxy);
	}
	

}
