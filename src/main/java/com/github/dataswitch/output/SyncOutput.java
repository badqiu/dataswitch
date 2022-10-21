package com.github.dataswitch.output;

import java.util.List;

public class SyncOutput extends ProxyOutput{

	public SyncOutput() {
		super();
	}

	public SyncOutput(Output proxy) {
		super(proxy);
	}

	@Override
	public synchronized void write(List<Object> rows) {
		super.write(rows);
	}
	
}
