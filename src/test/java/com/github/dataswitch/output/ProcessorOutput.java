package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.processor.Processor;

public class ProcessorOutput extends ProxyOutput {

	private Processor processor;
	
	public Processor getProcessor() {
		return processor;
	}

	public void setProcessor(Processor processor) {
		this.processor = processor;
	}

	@Override
	public void write(List<Object> rows) {
		try {
			List newRows = processor.process(rows);
			getProxy().write(newRows);
		}catch(Exception e) {
			throw new RuntimeException("error on processor:"+processor,e);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		processor.open(params);
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		processor.close();
	}
	
}
