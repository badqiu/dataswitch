package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;

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
	
	public void setProcessor(Processor processor) {
		setProcessors(processor);
	}
	
	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		if(ArrayUtils.isEmpty(processors)) return datas;
		if(CollectionUtils.isEmpty(datas)) return datas;
		
		return process0(datas);
	}

	private List<Object> process0(List<Object> datas) throws Exception {
		List<Object> tempDatas = datas;
		for(Processor p : processors) {
			tempDatas = p.process(tempDatas);
			
			if(CollectionUtils.isEmpty(tempDatas)) {
				break;
			}
		}
		return tempDatas;
	}
	
//	private List<Object> process1(List<Object> datas) throws Exception {
//		List result = new ArrayList(datas.size());
//		
//		for(Object row : datas) {
//			List rowList = processRowByProcessors(row);
//			if(CollectionUtils.isNotEmpty(rowList)) {
//				result.addAll(rowList);
//			}
//		}
//		
//		return result;
//	}
//
//	private List processRowByProcessors(Object row) throws Exception {
//		List rowList = Arrays.asList(row);
//		for(Processor p : processors) {
//			rowList = p.process(rowList);
//		}
//		return rowList;
//	}

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
