package com.github.dataswitch.storage;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.output.ProxyOutput;
import com.github.dataswitch.output.TeeOutput;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 输入输出类，一个输入可以配置多个输出
 * 
 * @author badqiu
 *
 */
public class InputsOutputs {

	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);

	private String id; // ID
	private String desc; // 描述
	private String author; // 作者
	private Input[] inputs;
	private Output[] outputs;
	private Output[] filters;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}
	
	public void setInput(Input input) {
		setInputs(input);
	}
	
	public void setInputs(Input... inputs) {
		this.inputs = inputs;
	}
	
	public Input[] getInputs() {
		return inputs;
	}

	public Output[] getOutputs() {
		return outputs;
	}

	public void setOutputs(Output... outputs) {
		this.outputs = outputs;
	}
	
	public void setOutput(Output output) {
		setOutputs(output);
	}

	public void exec() {
		MultiInput input = new MultiInput(inputs);
		TeeOutput output = new TeeOutput(outputs);
		
		int rows = InputOutputUtil.copy(input, output);
		logger.info(id+" copy success,rows:" + rows + " inputs:" + Arrays.toString(inputs)
				+ " outputs:" + Arrays.toString(outputs));
	}

	public void execByStorage() {
		MultiInput input = new MultiInput(inputs);
		Output output = new TeeOutput(outputs);
		if(ArrayUtils.isNotEmpty(filters)) {
			output = newFilterOutput(filters,output,0);
		}
		
		Storage storage = new Storage();
		List<Object> rows = null;
		if(!storage.isInputStored(id)) {
			while(CollectionUtils.isNotEmpty((rows = input.read(3000)))) {
				storage.write(rows);
			}
		}
		
		while((CollectionUtils.isNotEmpty(rows = storage.read(3000)))) {
			output.write(rows);
		}
		
	}

	private Output newFilterOutput(Output[] filters,Output lastOutput,int index) {
		if(index > filters.length) {
			return lastOutput;
		}
		return new ProxyOutput(newFilterOutput(filters,lastOutput,index + 1));
	}
}
