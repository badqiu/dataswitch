package com.github.dataswitch.util;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.FileInput;
import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.FileOutput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.output.ProxyOutput;
import com.github.dataswitch.output.TeeOutput;
import com.github.dataswitch.processor.DefaultProcessor;
import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;
import com.github.dataswitch.serializer.ByteDeserializer;
import com.github.dataswitch.serializer.ByteSerializer;

/**
 * 输入输出类，一个输入可以配置多个输出
 * 
 * 数据处理流程:
 * 
 * Input => Processor => Output
 * 
 * @author badqiu
 *
 */
public class InputsOutputs {

	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);

	private String id; // ID
	private String desc; // 描述
	private String author; // 作者
	private Input[] inputs; //输入
	private Output[] outputs; //输出
	private Processor[] processors;//数据处理器
	private int bufferSize = 5000;
	
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
	
	public Processor[] getProcessors() {
		return processors;
	}

	public void setProcessors(Processor... processors) {
		this.processors = processors;
	}

	public void setProcessor(Processor processor) {
		if(processor != null) {
			setProcessors(processor);
		}
	}
	
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void exec() {
		if(bufferSize <= 0) bufferSize = 5000;
		
		MultiInput input = new MultiInput(inputs);
		TeeOutput output = new TeeOutput(outputs);
		Processor processor = null;
		if(processors != null) {
			processor = new MultiProcessor(processors);
		}
		
		try {
			int rows = InputOutputUtil.copy(input, output,bufferSize,processor);
			logger.info(id+" copy success,rows:" + rows + " bufferSize:"+ bufferSize +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs));
		}finally {
			InputOutputUtil.closeQuietly(input);
			InputOutputUtil.closeQuietly(output);
		}
	}

}
