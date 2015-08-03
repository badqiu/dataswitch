package com.github.dataswitch.storage;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
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
import com.github.dataswitch.serializer.ByteDeserializer;
import com.github.dataswitch.serializer.ByteSerializer;
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
	private Input[] inputs; //输入
	private Output[] outputs; //输出
	private Output[] filters;
	private int bufferSize = 5000;
	private File storageDir;
	
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
	
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void setOutput(Output output) {
		setOutputs(output);
	}
	

	public void exec() {
		if(bufferSize <= 0) bufferSize = 5000;
//		Assert.isTrue(bufferSize > 0,"bufferSize > 0 must be true");
		
		MultiInput input = new MultiInput(inputs);
		TeeOutput output = new TeeOutput(outputs);
		
		int rows = InputOutputUtil.copy(input, output,bufferSize);
		logger.info(id+" copy success,rows:" + rows + " bufferSize:"+ bufferSize +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs));
		
		InputOutputUtil.closeQuietly(input);
		InputOutputUtil.closeQuietly(output);
	}

	public void execByStorage() {
		MultiInput input = new MultiInput(inputs);
		Output output = new TeeOutput(outputs);
		if(ArrayUtils.isNotEmpty(filters)) {
			output = newFilterOutput(filters,output,0);
		}
		
		List<Object> rows = null;
		if(storageDir != null) {
			FileOutput fileOutput = new FileOutput();
			fileOutput.setDir(storageDir.getAbsolutePath());
			fileOutput.setCompressType("gzip");
			fileOutput.setSerializer(new ByteSerializer());
			InputOutputUtil.copy(input, fileOutput);
		}
		
		FileInput fileInput = new FileInput();
		fileInput.setDir(storageDir.getAbsolutePath());
		fileInput.setDeserializer(new ByteDeserializer());
		InputOutputUtil.copy(fileInput, output);
	}

	private Output newFilterOutput(Output[] filters,Output lastOutput,int index) {
		if(index > filters.length) {
			return lastOutput;
		}
		return new ProxyOutput(newFilterOutput(filters,lastOutput,index + 1));
	}
}
