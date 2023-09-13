package com.github.dataswitch.processor;

import java.util.List;

import com.github.dataswitch.output.Output;
import com.github.dataswitch.util.ObjectSqlQueryUtil;

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

}
