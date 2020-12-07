package com.github.dataswitch.util;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.output.TeeOutput;
import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;

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

	private static final int DEFAULT_BUFFER_SIZE = 5000;

	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);

	private String id; // ID
	private String desc; // 描述
	private String author; // 作者
	private Input[] inputs; //输入
	private Output[] outputs; //输出
	private Processor[] processors;//数据处理器
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	
	private String failMode = FailMode.FAIL_AT_END.getShortName();
	
	/**
	 * 是否异步拷贝数据，默认是false
	 */
	private boolean async = false;
	
	/** 是否激活 */
	private boolean enabled = true;
	
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
	
	public String getFailMode() {
		return failMode;
	}

	public void setFailMode(String failMode) {
		this.failMode = failMode;
	}
	
	public boolean isAsync() {
		return async;
	}

	public void setAsync(boolean async) {
		this.async = async;
	}
	
	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public void exec() {
		if(bufferSize <= 0) bufferSize = DEFAULT_BUFFER_SIZE;
		
		if(!enabled) {
			throw new IllegalStateException("enabled is false, id:"+id);
		}
		
		MultiInput input = new MultiInput(inputs);
		TeeOutput output = new TeeOutput(outputs);
		Processor processor = null;
		if(processors != null) {
			processor = new MultiProcessor(processors);
		}
		
		long start = System.currentTimeMillis();
		int rows = 0;
		try {
			if(async) {
				rows = InputOutputUtil.asyncCopy(input,output,bufferSize,processor,failMode);
			}else {
				rows = InputOutputUtil.copy(input, output,bufferSize,processor,failMode);
			}
			long cost = System.currentTimeMillis() - start;
			logger.info(id+" copy success,rows:" + rows +" costSeconds:"+(cost / 1000) + " tps:"+(rows * 1000.0 / cost) + " bufferSize:"+ bufferSize+" failMode:" + failMode +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs));
		}finally {
			InputOutputUtil.closeQuietly(input);
			InputOutputUtil.closeQuietly(output);
		}
	}


}
