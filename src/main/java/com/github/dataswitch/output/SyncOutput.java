package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

public class SyncOutput extends ProxyOutput{

	public SyncOutput() {
		super();
	}

	public SyncOutput(Output proxy) {
		super(proxy);
	}

	@Override
	public synchronized void write(List<Map<String, Object>> rows) {
		super.write(rows);
	}
	
}
