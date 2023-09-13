package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;

public class ProcessorInput extends ProxyInput {

	private Processor[] processors;
	private MultiProcessor _multiProcessor;
	
	public ProcessorInput() {
		super();
	}

	public ProcessorInput(Input proxy) {
		super(proxy);
	}

	public ProcessorInput(Input proxy,Processor... processor) {
		super(proxy);
		setProcessors(processor);
	}
	

	public Processor[] getProcessors() {
		return processors;
	}

	public void setProcessors(Processor... processors) {
		this.processors = processors;
	}
	
	public void setProcessor(Processor p) {
		setProcessors(p);
	}

	@Override
	public List read(int size) {
		try {
			List list = getProxy().read(size);
			return _multiProcessor.process(list);
		}catch(Exception e) {
			throw new RuntimeException("error on processor:"+_multiProcessor,e);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		_multiProcessor = new MultiProcessor(processors);
		_multiProcessor.open(params);
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		_multiProcessor.close();
	}
	
}
