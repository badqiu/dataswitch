package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Enabled;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 支持多个输入的Input的代理,实现从多个流读数据
 * 
 * @author badqiu
 *
 */
public class MultiInput extends BaseObject implements Input{

	private List<Input> inputs = new ArrayList<Input>();
	
	private boolean concurrent = false; //并发读

	
	private transient Input _currentInput;
	private AtomicInteger _currentIndex = new AtomicInteger();
	private ExecutorService executorService = null;
	
	public MultiInput() {
	}
	
	public MultiInput(List<Input> inputs) {
		setInputs(inputs);
	}
	
	public MultiInput(Input... inputs) {
		setInputs(inputs);
	}
	
	public void setInput(Input input) {
		setInputs(input);
	}
	
	public void setInputs(List<Input> inputs) {
		this.inputs = inputs;
	}
	
	public void setInputs(Input... inputs) {
		if(inputs == null) return;
		
		setInputs(new ArrayList<Input>(Arrays.asList(inputs)));
	}
	
	public void addInput(Input input) {
		inputs.add(input);
	}
	
	public boolean isConcurrent() {
		return concurrent;
	}

	public void setConcurrent(boolean concurrent) {
		this.concurrent = concurrent;
	}
	
	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void commitInput() {
		for(Input input : inputs) {
			input.commitInput();
		}
	}
	
	@Override
	public void close() throws Exception {
		if(executorService != null) {
			executorService.shutdown();
			executorService.awaitTermination(Constants.ON_CLOSE_EXECUTOR_AWAIT_TERMINATION_SECOND, TimeUnit.SECONDS);
		}
		
		InputOutputUtil.closeAllQuietly(inputs);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		this.inputs = Enabled.filterByEnabled(inputs);
		
		if(concurrent) {
			if(executorService == null) {
				executorService = Executors.newFixedThreadPool(3);
			}
			_inputReadEnd = new boolean[inputs.size()];
//			this.inputs = toAsyncInputs(inputs);
		}
		
		InputOutputUtil.openAll(params, this.inputs);
	}

	public static List<Input> toAsyncInputs(List<Input> inputs) {
		List<Input> asyncInputs = new ArrayList<Input>();
		for(int i = 0; i < inputs.size(); i++) {
			asyncInputs.add(new AsyncInput(inputs.get(i)));
		}
		return asyncInputs;
	}

	@Override
	public List<Object> read(final int size) {
		if(concurrent) {
			try {
				return concurrentRead(size);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}else {
			return sequenceRead(size);
		}
	}

	private boolean[] _inputReadEnd = null;
	protected List<Object> concurrentRead(final int size) throws InterruptedException, ExecutionException {
		List all = new ArrayList();
		
		for(int i = 0; i < inputs.size(); i++) {
			Input input = inputs.get(i);
			boolean readEnd = _inputReadEnd[i];
			if(readEnd) {
				continue;
			}
			
			Future<List> future = executorService.submit(new Callable<List>() {
				public List call() throws Exception {
					return input.read(size);
				}
			});
			
			List sublist = future.get();
			
			if(CollectionUtils.isEmpty(sublist)) {
				_inputReadEnd[i] = true;
			}else {
				all.addAll(sublist);
			}
		}
		
		return all;
	}
	

	private List<Object> sequenceRead(int size) {
		if(_currentInput == null) {
			int i = _currentIndex.get();
			if(i >= inputs.size()) {
				return Collections.EMPTY_LIST;
			}
			
			_currentInput = inputs.get(i);
			_currentIndex.incrementAndGet();
		}
		
		List<Object> result = _currentInput.read(size);
		if(CollectionUtils.isEmpty(result)) {
			_currentInput = null;
			return read(size);
		}
		return result;
	}

}
