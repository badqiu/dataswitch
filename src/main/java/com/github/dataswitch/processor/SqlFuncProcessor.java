package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.CollectionUtil;
import com.github.dataswitch.util.Util;

/**
 * 提供类似SQL的where,select等过滤数据功能
 * 
 * @author badqiu
 *
 */
public class SqlFuncProcessor extends BaseProcessor{
	private static Logger logger = LoggerFactory.getLogger(SqlFuncProcessor.class);
	// where columns
	// select columns
	// remove columns
	// order by columns
	// group by columns, having
	// limit offset,limit
	
	private String where; //过滤数据,语法使用MVEL，类似java条件表达式
	private String select; //要select的列,如果没有则选择所有列
	private String remove; //要删除的列,如果没有不移除列
	private long limit; //
	private long offset; //从0开始
	
	private String orderBy;
	private String groupBy;
//	private String having;
	private String print; //条件成立时打印日志
	
	
	private String[] _removeKeys = null;
	private String[] _selectKeys = null;
	private String[] _groupByKeys = null;
	private long _limitCount = 0;
	private long _OffsetCount = 0;
	
	
	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getSelect() {
		return select;
	}

	public void setSelect(String select) {
		this.select = select;
	}

	public String getRemove() {
		return remove;
	}

	public void setRemove(String remove) {
		this.remove = remove;
	}
	
	public String getPrint() {
		return print;
	}

	public void setPrint(String print) {
		this.print = print;
	}

	public long getLimit() {
		return limit;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
	
	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	@Override
	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception {
		List<Map> list = (List)super.process(datas);
		
		list = groupBy(list);
		return (List)orderBy(list);
	}

	private List<Map> groupBy(List<Map> list) {
		if(ArrayUtils.isEmpty(_groupByKeys)) {
			return list;
		}
		
		Map r = list.stream().collect(Collectors.groupingBy((o) -> {
			return buildKey(o,_groupByKeys);
		}));
		return Arrays.asList(r);
	}

	private Object buildKey(Map o, String[] keys) {
		if(keys == null) return null;
		if(keys.length == 1) return o.get(keys[0]);
		
		List values =new ArrayList();
		for(String key : keys) {
			Object value = o.get(key);
			values.add(value);
		}
		return values;
	}

	private List<Map> orderBy(List<Map> list) {
		if(StringUtils.isBlank(orderBy))
			return list;
		
		String[] array = orderBy.split("\\s+");
		String orderByKey = array[0];
		String sortOrderStr = array.length >= 2 ? array[1] : null;
		CollectionUtil.sort(list, (input) -> {
			if(input == null) return null;
			
			return input.get(orderByKey);
		}, sortOrderStr);
		return list;
	}

	@Override
	protected Map<String,Object> processOne(Map<String,Object> row) throws Exception {
		if(row == null) return null;
		
		Map map = (Map)row;
		
		return processMap(map);
	}

	private Map<String,Object> processMap(Map map) {
		if(offset > 0) {
			_OffsetCount++;
			if(_OffsetCount <= offset) {
				return null;
			}
		}
		
		if(limit > 0) {
			_limitCount++;
			if(_limitCount > limit) {
				return null;
			}
		}
		
		if(StringUtils.isNotBlank(print)) {
			Boolean pass = (Boolean)MVEL.eval(print, map);
			if(pass) {
				logger.info("show_log_by_print:"+String.valueOf(map));
			}
		}
		
		if(StringUtils.isNotBlank(where)) {
			Boolean pass = (Boolean)MVEL.eval(where, map);
			if(!pass) {
				return null;
			}
		}
		
		if(ArrayUtils.isNotEmpty(_removeKeys)) {
			for(String key : _removeKeys) {
				map.remove(key);
			}
		}
		
		if(ArrayUtils.isNotEmpty(_selectKeys)) {
			return selectByKeys(map,_selectKeys);
		}
		
		return map;
	}

	private Map<String,Object> selectByKeys(Map map,String[] keys) {
//		if(true) {
//			Map result = new HashMap();
//			for(String key : keys) {
//				Object value = MVEL.eval(key,map);
//				result.put(key,value);
//			}
//			return result;
//		}
		
		
		Map result = new LinkedHashMap(keys.length * 2);
		for(String key : keys) {
			Object value = map.get(key);
			
			if(value != null) {
				result.put(key, value);
			}
		}
		return result;
	}
	
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
//		where = convertSqlWhere2JavaWhere(where);
		_removeKeys = Util.splitColumns(remove);
		_selectKeys = Util.splitColumns(select);
		_groupByKeys = Util.splitColumns(groupBy);
	}

	private String convertSqlWhere2JavaWhere(String where) {
		if(StringUtils.isBlank(where)) return null;
		
		String result = where.trim();
		result = result.replaceAll("(?i)\sand\s", " && ");
		result = result.replaceAll("(?i)\sor\s", " || ");

		result = result.replaceAll("(?i)\snot\s+", " ! ");
		if(result.toLowerCase().startsWith("not")) {
			result = result.replaceAll("(?i)not\s+", " ! ");
		}
		return result;
	}
}
