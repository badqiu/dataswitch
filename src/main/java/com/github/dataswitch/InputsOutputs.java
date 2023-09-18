package com.github.dataswitch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

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
import com.github.dataswitch.util.InputOutputUtil.CopyResult;
import com.github.dataswitch.util.ScriptEngineUtil;

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
public class InputsOutputs extends BaseObject implements Enabled,Runnable,Callable<Long>,Function<Map<String,Object>, Long>,Openable,Closeable,InitializingBean,DisposableBean {


	private static Logger logger = LoggerFactory.getLogger(InputsOutputs.class);
	
	private String desc; // 描述
	private String author; // 作者
	
	private int batchSize = Constants.DEFAULT_BATCH_SIZE;
	private int batchTimeout = Constants.DEFAULT_BATCH_TIMEOUT; //超时时间，时间单位毫秒
	
	private FailMode failMode = FailMode.FAIL_FAST;
	
	private Input[] inputs; //输入
	private Output[] outputs; //输出
	private Processor[] processors;//数据处理器
	
	private Consumer<Exception> exceptionHandler; //异常处理器
	/**
	 * 是否异步拷贝数据，默认是false
	 */
	private boolean async = false;
	
	/**
	 * 未处理任何数据结束时，报错
	 */
	private boolean errorOnNoData = false;
	
	
	private Map<String,Object> params = new HashMap<String,Object>(); //运行参数，在没有外部参数传递时使用
	
	private String language; //执行的脚本语言，如groovy
	private String initScript; //初始化执行的脚本
	private String destoryScript; //destory() 时执行的脚本
	
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
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public String getFailMode() {
		return failMode.name();
	}

	public void setFailMode(String failMode) {
		this.failMode = FailMode.getRequiredByName(failMode);
	}
	
	public void failMode(FailMode failMode) {
		this.failMode = failMode;
	}
	
	public void setFailMode(FailMode failMode) {
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
	
	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	
	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getInitScript() {
		return initScript;
	}

	public void setInitScript(String initScript) {
		this.initScript = initScript;
	}

	public String getDestoryScript() {
		return destoryScript;
	}

	public void setDestoryScript(String destoryScript) {
		this.destoryScript = destoryScript;
	}
	
	public Consumer<Exception> getExceptionHandler() {
		return exceptionHandler;
	}

	public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}
	
	public boolean isErrorOnNoData() {
		return errorOnNoData;
	}

	public void setErrorOnNoData(boolean errorOnNoData) {
		this.errorOnNoData = errorOnNoData;
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
		if(batchSize <= 0) batchSize = Constants.DEFAULT_BATCH_SIZE;
		if(params == null) params = Collections.EMPTY_MAP;
		
		if(!isEnabled()) {
			throw new IllegalStateException("enabled is false, "+info());
		}
		
//		Assert.hasText(getId(),"id must be not blank");
//		Assert.hasText(getAuthor(),"author must be not blank");
//		Assert.hasText(getRemarks(),"remarks must be not blank");
//		Assert.hasText(getCreateDate(),"createDate must be not blank");
		
		try {
			afterPropertiesSet();
			
			Input input = new MultiInput(inputs);
			Output output = new MultiOutput(outputs);
			
			if(async) {
				output = new AsyncOutput(output);
			}
			
			if(batchSize > 0) {
				output = new BufferedOutput(output, batchSize, batchTimeout);
			}
			
			
			Processor processor = null;
			if(ArrayUtils.isNotEmpty(processors)) {
				processor = new MultiProcessor(processors);
			}
			
			long rows = exec(params, input, output, processor);
			if(errorOnNoData && rows == 0) {
				throw new RuntimeException("no any data write to output,data rows=0," +info());
			}
			return rows;
		}finally {
			destroy();
		}
	}

	private long exec(Map<String, Object> params, Input input, Output output, Processor processor) {
		long rows = 0;
		long costTime = 0;
		
		try {
			
			CopyResult copyResult = InputOutputUtil.copy(input, output,batchSize,processor,params,failMode,exceptionHandler);
			
			rows = copyResult.getCount();
			costTime = copyResult.getTotalCostTime();
			
			return rows;
		}catch(Exception e) {
			throw new RuntimeException(info() +" copy error",e);
		}finally {
			String msg = info() + " copy end,rows:" + rows +" costSeconds:"+(costTime / 1000) + " tps:"+(rows * 1000.0 / costTime) + " batchSize:"+ batchSize+" failMode:" + failMode +" inputs:" + Arrays.toString(inputs) + " outputs:" + Arrays.toString(outputs);
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

	@Override
	public void destroy()  {
		ScriptEngineUtil.eval(language, destoryScript);
	}

	@Override
	public void afterPropertiesSet()  {
		ScriptEngineUtil.eval(language, initScript);
	}

	@Override
	public void close() throws IOException {
		InputOutputUtil.closeAllQuietly(inputs);
		InputOutputUtil.closeAllQuietly(processors);
		InputOutputUtil.closeAllQuietly(outputs);
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		InputOutputUtil.openAll(params,inputs);
		InputOutputUtil.openAll(params,processors);
		InputOutputUtil.openAll(params,outputs);
	}

}
