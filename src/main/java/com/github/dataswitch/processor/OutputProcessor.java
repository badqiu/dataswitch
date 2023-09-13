package com.github.dataswitch.processor;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.output.Output;

public class OutputProcessor implements Processor {

	private Output output;

	public OutputProcessor(){
	}
	
	public OutputProcessor(Output output) {
		this.output = output;
	}
	
	public Output getOutput() {
		return output;
	}

	public void setOutput(Output output) {
		this.output = output;
	}

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		output.write(datas);
		return datas;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Processor.super.open(params);
		output.open(params);
	}
	
	@Override
	public void close() throws Exception {
		Processor.super.close();
		output.close();
	}
}
