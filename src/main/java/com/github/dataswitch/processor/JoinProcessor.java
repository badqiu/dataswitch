package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.Util;

/**
 * 将数据放在内存中进行join
 * join条件通过joinKeys指定，只能等值join
 * 
 * @author badqiu
 *
 */
public class JoinProcessor implements Processor {

	private Map<String,Map> _dataMap;
	
	private List<Map> dataList = null;
	private String joinKeys;
	private String[] _joinKeys;
	private boolean newMapForJoinResult = false;
	
	private Input input;
	
	private Function<String[],Map> lookupFunction;

	public String getJoinKeys() {
		return joinKeys;
	}

	public void setJoinKeys(String joinKeys) {
		this.joinKeys = joinKeys;
		_joinKeys = Util.splitColumns(joinKeys);
	}
	
	public List<Map> getDataList() {
		return dataList;
	}

	public void setDataList(List<Map> dataList) {
		this.dataList = dataList;
	}
	
	public Input getInput() {
		return input;
	}

	public void setInput(Input input) {
		this.input = input;
	}

	public boolean isNewMapForJoinResult() {
		return newMapForJoinResult;
	}

	public void setNewMapForJoinResult(boolean newMapForJoinResult) {
		this.newMapForJoinResult = newMapForJoinResult;
	}
	
	public Function<String[], Map> getLookupFunction() {
		return lookupFunction;
	}

	public void setLookupFunction(Function<String[], Map> lookupFunction) {
		this.lookupFunction = lookupFunction;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		init();
	}

	protected void init() {
		setJoinKeys(joinKeys);
		if(dataList == null) {
			if(input != null) {
				dataList = (List)InputOutputUtil.readFully(input, 1000);
			}
		}
		
		_dataMap = buildDataMapFromJoinDatas(dataList,_joinKeys);
	}
	
	private static Map<String, Map> buildDataMapFromJoinDatas(List<Map> joinDatas,String[] joinKeys) {
		if(ArrayUtils.isEmpty(joinKeys)) return null;
		if(CollectionUtils.isEmpty(joinDatas)) return null;
		
		Map resultMap = new HashMap();
		for(Map row : joinDatas) {
			String key = buildMapKey(joinKeys);
			resultMap.put(key, row);
		}
		return resultMap;
	}

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		return (List)processMaps((List)datas);
	}

	private List<Map> processMaps(List<Map> datas) {
		if(CollectionUtils.isEmpty(datas)) return datas;
		
		List<Map> result = new ArrayList(datas.size());
		for(Map row : datas) {
			Map newRow = join(row,_joinKeys);
			
			if(newRow != null) {
				result.add(newRow);
			}
			
		}
		return result;
	}

	private Map join(Map row, String[] joinKeys) {
		if(_dataMap == null) return null;
		
		Map joinData = lookupData(joinKeys);
		if(joinData == null) {
			return row;
		}
		
		
		if(newMapForJoinResult) {
			Map result = new HashMap(row);
			result.putAll(row);
			return result;
		}else {
			row.putAll(row);
			return row;
		}
	}

	protected Map lookupData(String[] joinKeys) {
		if(lookupFunction != null) {
			Map result = lookupFunction.apply(joinKeys);
			return result;
		}
		
		String key = buildMapKey(joinKeys);
		Map joinData = _dataMap.get(key);
		return joinData;
	}

	protected static String buildMapKey(String[] joinKeys) {
		String key = StringUtils.join(joinKeys,"#");
		return key;
	}

}
