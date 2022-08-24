package com.github.dataswitch.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Constants;
import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.MultiOutput;
import com.github.dataswitch.output.Output;
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
public class InputsOutputs extends BaseObject {


	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);

	private int bufferSize = Constants.DEFAULT_BUFFER_SIZE;
	private String failMode = FailMode.FAIL_AT_END.getShortName();

	private String desc; // 描述
	private String author; // 作者
	
	private Input[] inputs; //输入
	private Output[] outputs; //输出
	private Processor[] processors;//数据处理器
	
	private Consumer<Exception> exceptionHandler; //异常处理器
	/**
	 * 是否异步拷贝数据，默认是false
	 */
	private boolean async = false;
	
	/** 是否激活 */
	private boolean enabled = true;
	
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
	
	public String info() {
		return "id:"+getId();
	}

	public void exec() {
		exec(Collections.EMPTY_MAP);
	}
	
	public void exec(Map<String,Object> params) {
		if(bufferSize <= 0) bufferSize = Constants.DEFAULT_BUFFER_SIZE;
		if(params == null) params = Collections.EMPTY_MAP;
		
		if(!enabled) {
			throw new IllegalStateException("enabled is false, "+info());
		}
		
		MultiInput input = new MultiInput(inputs);
		MultiOutput output = new MultiOutput(outputs);
		Processor processor = null;
		if(processors != null) {
			processor = new MultiProcessor(processors);
		}
		
		exec(params, input, output, processor);
	}

	private void exec(Map<String, Object> params, MultiInput input, MultiOutput output, Processor processor) {
		long start = System.currentTimeMillis();
		long rows = 0;
		long costTime = 0;
		try {
			input.open(params);
			output.open(params);
			
			FailMode failModeEnum = FailMode.getRequiredByName(failMode);
			if(async) {
				rows = InputOutputUtil.asyncCopy(input,output,bufferSize,processor,failModeEnum,exceptionHandler);
			}else {
				rows = InputOutputUtil.copy(input, output,bufferSize,processor,failModeEnum,exceptionHandler);
			}
			costTime = System.currentTimeMillis() - start;
		}catch(Exception e) {
			throw new RuntimeException(info() +" copy error",e);
		}finally {
			InputOutputUtil.closeQuietly(input);
			InputOutputUtil.closeQuietly(output);
			
			String msg = info() + " copy end,rows:" + rows +" costSeconds:"+(costTime / 1000) + " tps:"+(rows * 1000.0 / costTime) + " bufferSize:"+ bufferSize+" failMode:" + failMode +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs);
			logger.info(msg);
		}
	}


}
