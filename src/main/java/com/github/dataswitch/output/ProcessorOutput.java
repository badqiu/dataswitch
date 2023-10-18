package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;
import com.github.dataswitch.util.InputOutputUtil;

public class ProcessorOutput extends ProxyOutput {

	private Processor[] processors;
	private MultiProcessor _multiProcessor;
	
	
	public ProcessorOutput() {
		super();
	}

	public ProcessorOutput(Output proxy) {
		super(proxy);
	}

	public ProcessorOutput(Output proxy,Processor... processor) {
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
	public void write(List<Map<String, Object>> rows) {
		try {
			List newRows = _multiProcessor.process(rows);
			getProxy().write(newRows);
		}catch(Exception e) {
			throw new RuntimeException("error on processor:"+_multiProcessor,e);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		_multiProcessor = new MultiProcessor(processors);
		InputOutputUtil.open(params, _multiProcessor);
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		InputOutputUtil.close(_multiProcessor);
	}
	
}
