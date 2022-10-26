package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Enabled;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 支持多个输入的Input的代理,实现从多个流读数据
 * 
 * @author badqiu
 *
 */
public class MultiInput extends BaseObject implements Input{

	private List<Input> inputs = new ArrayList<Input>();
	
	private transient Input currentInput;
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
		this.inputs = Enabled.filterByEnabled(inputs);
		
		if(concurrentRead) {
			List<Input> asyncInputs = new ArrayList<Input>();
			for(int i = 0; i < inputs.size(); i++) {
				asyncInputs.add(new AsyncInput(inputs.get(i)));
			}
			this.inputs = asyncInputs;
		}
		
		InputOutputUtil.openAll(params, this.inputs);
	}

	@Override
	public List<Object> read(int size) {
		return sequenceRead(size);
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
