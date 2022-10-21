package com.github.dataswitch.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.util.InputOutputUtil;
/**
 * 多个Processor合成一个Processor,按顺序处理数据
 * 
 * @author badqiu
 *
 */
public class MultiProcessor implements Processor{

	private Processor[] processors;

	public MultiProcessor(){
	}
	
	public MultiProcessor(Processor... processors) {
		super();
		this.processors = processors;
	}

	public Processor[] getProcessors() {
		return processors;
	}

	public void setProcessors(Processor... processors) {
		this.processors = processors;
	}

	public void setProcessors(List<Processor> processors) {
		this.processors = processors.toArray(new Processor[0]);
	}
	
	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		List<Object> tempDatas = datas;
		for(Processor p : processors) {
			tempDatas = p.process(tempDatas);
		}
		return tempDatas;
	}

	@Override
	public void close() throws Exception {
		for(Processor p : processors) {
			InputOutputUtil.close(p);
		}
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		processors = Enabled.filterByEnabled(processors);
		
		InputOutputUtil.openAll(params, processors);
	}
	
	
}
