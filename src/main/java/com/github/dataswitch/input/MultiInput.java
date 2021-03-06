package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

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
	private int currentIndex;
	
	public MultiInput() {
	}
	
	public MultiInput(List<Input> inputs) {
		setInputs(inputs);
	}
	
	public MultiInput(Input... inputs) {
		setInputs(inputs);
	}
	
	public void setInput(Input input) {
		this.inputs = new ArrayList<Input>(Arrays.asList(input));
	}
	
	public void setInputs(List<Input> inputs) {
		this.inputs = inputs;
	}
	
	public void setInputs(Input... inputs) {
		this.inputs = new ArrayList<Input>(Arrays.asList(inputs));
	}
	
	public void addInput(Input input) {
		inputs.add(input);
	}
	
	@Override
	public void close() throws IOException {
		for(Input input : inputs) {
			InputOutputUtil.closeQuietly(input);
		}
	}

	@Override
	public List<Object> read(int size) {
		if(currentInput == null) {
			if(currentIndex >= inputs.size()) {
				return Collections.EMPTY_LIST;
			}
			
			currentInput = inputs.get(currentIndex);
			currentIndex++;
		}
		
		List<Object> result = currentInput.read(size);
		if(CollectionUtils.isEmpty(result)) {
			currentInput = null;
			return read(size);
		}
		return result;
	}

}
