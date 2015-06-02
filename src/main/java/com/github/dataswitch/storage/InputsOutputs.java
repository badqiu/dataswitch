package com.github.dataswitch.storage;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.Output;
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
		TeeOutput output = new TeeOutput(outputs);
		int rows = InputOutputUtil.copy(new MultiInput(inputs), output, true);
		logger.info("copy success,rows:" + rows + " inputs:" + Arrays.toString(inputs)
				+ " outputs:" + Arrays.toString(outputs));
	}

}
