package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.util.InputOutputUtil;

public class MultiProcessorOneProcessor implements Processor{

	private ProcessOneProcessor[] processors;
	private FailMode failMode = FailMode.FAIL_FAST;
	
	public MultiProcessorOneProcessor() {
	}
	
	public MultiProcessorOneProcessor(ProcessOneProcessor... processors) {
		super();
		setProcessors(processors);
	}


	public ProcessOneProcessor[] getProcessors() {
		return processors;
	}

	public void setProcessors(ProcessOneProcessor... processors) {
		this.processors = processors;
	}

	public void setProcessor(ProcessOneProcessor processor) {
		setProcessors(processor);
	}
	
	public FailMode getFailMode() {
		return failMode;
	}

	public void setFailMode(FailMode failMode) {
		this.failMode = failMode;
	}

	@Override
	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception {
		if(CollectionUtils.isEmpty(datas)) return datas;
		
		List<Map<String,Object>> result = new ArrayList<Map<String,Object>>(datas.size());
		
		for(Map<String,Object> row : datas) {
			for(ProcessOneProcessor p : processors) {
				row = p.processOne(row);
			}
			result.add(row);
		}
		
		return result;
	}
	
	@Override
	public void close() throws Exception {
		InputOutputUtil.closeAll(failMode,processors);
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		processors = Enabled.filterByEnabled(processors);
		
		InputOutputUtil.openAll(failMode,params, processors);
	}

}
