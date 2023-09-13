package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.github.dataswitch.util.Util;

public class JoinProcessor implements Processor {

	private Map<String,Map> _dataMap;
	
	private List<Map> joinDatas = new ArrayList();
	private String joinKeys;
	private String[] _joinKeys;
	

	public String getJoinKeys() {
		return joinKeys;
	}

	public void setJoinKeys(String joinKeys) {
		this.joinKeys = joinKeys;
		_joinKeys = Util.splitColumns(joinKeys);
	}
	
	public List<Map> getJoinDatas() {
		return joinDatas;
	}

	public void setJoinDatas(List<Map> joinDatas) {
		this.joinDatas = joinDatas;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		init();
	}

	protected void init() {
		setJoinKeys(joinKeys);
		_dataMap = buildDataMapFromJoinDatas(joinDatas,_joinKeys);
	}
	
	private static Map<String, Map> buildDataMapFromJoinDatas(List<Map> joinDatas,String[] joinKeys) {
		if(ArrayUtils.isEmpty(joinKeys)) return new HashMap();
		if(CollectionUtils.isEmpty(joinDatas)) return new HashMap();
		
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
		
		String key = buildMapKey(joinKeys);
		Map joinData = _dataMap.get(key);
		if(joinData == null) {
			return row;
		}
		
//		Map result = new HashMap(row);
//		result.putAll(row);
//		return result;
		
		row.putAll(row);
		return row;
	}

	protected static String buildMapKey(String[] joinKeys) {
		String key = StringUtils.join(joinKeys,"$");
		return key;
	}

}
