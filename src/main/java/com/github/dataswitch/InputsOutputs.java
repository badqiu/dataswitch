package com.github.dataswitch;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.AsyncOutput;
import com.github.dataswitch.output.BufferedOutput;
import com.github.dataswitch.output.MultiOutput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;
import com.github.dataswitch.util.InputOutputUtil;

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
public class InputsOutputs extends BaseObject implements Enabled,Runnable,Callable<Long>,Function<Map<String,Object>, Long> {


	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);

	private int bufferSize = Constants.DEFAULT_BUFFER_SIZE;
	private int bufferTimeout = Constants.DEFAULT_BUFFER_TIMEOUT; //超时时间，时间单位毫秒
	
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
	
	private Map<String,Object> params = new HashMap<String,Object>();
	
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
	
	public int getBufferTimeout() {
		return bufferTimeout;
	}

	public void setBufferTimeout(int bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
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
	
	public void setSync(boolean sync) {
		this.async = !sync;
	}
	
	public boolean isSync() {
		return !isAsync();
	}
	
	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}

	public String info() {
		return "id:"+getId();
	}

	public long exec() {
		return exec(this.params);
	}
	
	/**
	 *  执行任务
	 * @param params 任务参数
	 * @return 数据行数
	 */
	public long exec(Map<String,Object> params) {
		if(bufferSize <= 0) bufferSize = Constants.DEFAULT_BUFFER_SIZE;
		if(params == null) params = Collections.EMPTY_MAP;
		
		if(!enabled) {
			throw new IllegalStateException("enabled is false, "+info());
		}
		
		Input input = new MultiInput(inputs);
		Output output = new MultiOutput(outputs);
		
		if(async) {
			output = new AsyncOutput(output);
		}
		
		if(bufferSize > 0) {
			output = new BufferedOutput(output, bufferSize, bufferTimeout);
		}
		
		
		Processor processor = null;
		if(processors != null) {
			processor = new MultiProcessor(processors);
		}
		
		return exec(params, input, output, processor);
	}

	private long exec(Map<String, Object> params, Input input, Output output, Processor processor) {
		long rows = 0;

		long start = System.currentTimeMillis();
		long costTime = 0;
		try {
			
			FailMode failModeEnum = FailMode.getRequiredByName(failMode);
//			if(async) {
//				rows = InputOutputUtil.asyncCopy(input,output,bufferSize,processor,params,failModeEnum,exceptionHandler);
//			}else {
//				rows = InputOutputUtil.copy(input, output,bufferSize,processor,params,failModeEnum,exceptionHandler);
//			}
			
			rows = InputOutputUtil.copy(input, output,bufferSize,processor,params,failModeEnum,exceptionHandler);
			costTime = System.currentTimeMillis() - start;
			
			return rows;
		}catch(Exception e) {
			throw new RuntimeException(info() +" copy error",e);
		}finally {
			String msg = info() + " copy end,rows:" + rows +" costSeconds:"+(costTime / 1000) + " tps:"+(rows * 1000.0 / costTime) + " bufferSize:"+ bufferSize+" failMode:" + failMode +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs);
			logger.info(msg);
		}
	}

	@Override
	public void run() {
		exec();
	}
	
	@Override
	public Long call() throws Exception {
		return exec();
	}

	@Override
	public Long apply(Map<String, Object> param) {
		return exec(param);
	}


}
