package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;

/**
 * 多功能的Output
 * 
 **/
public class MultiFunctionOutput extends ProxyOutput {

	private boolean async = false; //done
	private boolean sync = false; //done
	private boolean lock = false;
	private boolean buffered = false; 
	private boolean retry = false; 
	private boolean nullOutput = false; //done
	private boolean print = false; //done
	
	private Consumer<List<Object>> consumer; //done
	private String logger = null; //done
	
	public MultiFunctionOutput() {
		super();
	}

	public MultiFunctionOutput(Output proxy) {
		setProxy(proxy);
	}
	
	@Override
	public void setProxy(Output proxy) {
		super.setProxy(proxy);
	}
	
	public void setConsumer(Consumer<List<Object>> consumer) {
		this.consumer = consumer;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	private Output newMultiFunctionProxy(Output proxy) {
		Output output = proxy;
		
		if(consumer != null) {
			output = new ComsumerOutput(consumer);
		}
		if(StringUtils.isNotBlank(logger)) {
			LoggerOutput loggerOutput = new LoggerOutput();
			loggerOutput.setLogger(logger);
			output = loggerOutput;
		}
		if(nullOutput) {
			output = new NullOutput();
		}
		if(print) {
			output = new PrintOutput();
		}
		
		if(buffered) {
			output = new BufferedOutput(output);
		}
		if(retry) {
			output = new RetryOutput(output);
		}
		if(lock) {
			output = new LockOutput(output);
		}
		if(sync) {
			output = new SyncOutput(output);
		}
		if(async) {
			output = new AsyncOutput(output);
		}
		
		
		return output;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		Output newProxy = newMultiFunctionProxy(getProxy());
		setProxy(newProxy);
	}
	

}
