package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 支持多个输入的Input的代理,实现从多个流读数据
 * 
 * @author badqiu
 *
 */
public class MultiInput implements Input{

	private List<Input> inputs = new ArrayList<Input>();
	
	private Input currentInput;
	private AtomicInteger currentIndex = new AtomicInteger();
	
	private boolean concurrentRead = false; //并发读
	
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
	
	public boolean isConcurrentRead() {
		return concurrentRead;
	}

	public void setConcurrentRead(boolean concurrentRead) {
		this.concurrentRead = concurrentRead;
	}

	@Override
	public void commitInput() {
		for(Input input : inputs) {
			input.commitInput();
		}
	}
	
	@Override
	public void close() throws Exception {
		InputOutputUtil.closeAllQuietly(inputs);
	}
	
	
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		inputs = Enabled.filterByEnabled(inputs);
		InputOutputUtil.openAll(params, inputs);
	}

	@Override
	public List<Object> read(int size) {
		if(concurrentRead) {
			return concurrentRead(size);
		}else {
			return sequenceRead(size);
		}
	}
	
	private List<Object> concurrentRead(int size) {
		throw new UnsupportedOperationException();
	}

	private List<Object> sequenceRead(int size) {
		if(currentInput == null) {
			int i = currentIndex.get();
			if(i >= inputs.size()) {
				return Collections.EMPTY_LIST;
			}
			
			currentInput = inputs.get(i);
			currentIndex.incrementAndGet();
		}
		
		List<Object> result = currentInput.read(size);
		if(CollectionUtils.isEmpty(result)) {
			currentInput = null;
			return read(size);
		}
		return result;
	}

}
