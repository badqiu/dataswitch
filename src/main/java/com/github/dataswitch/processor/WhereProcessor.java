package com.github.dataswitch.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;

import com.github.dataswitch.util.Util;

public class WhereProcessor extends BaseProcessor{
	// where columns
	// select columns
	// remove columns
	// order by columns
	// group by columns, having
	// limit offset,limit
	
	private String where; //done   age > 10 && name == 'badqiu'
	private String select; //done
	private String remove; //done
	private int limit; //done
	private int offset; //done
	
	private String orderBy;
	private String groupBy;
	private String having;
	
	
	private String[] _removeKeys = null;
	private String[] _selectKeys = null;
	private int _limitCount = 0;
	private int _OffsetCount = 0;
	
	@Override
	protected Object processOne(Object row) throws Exception {
		if(row == null) return null;
		
		Map map = (Map)row;
		
		return processMap(map);
	}

	private Object processMap(Map map) {
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
			Map result = new LinkedHashMap(_selectKeys.length * 2);
			for(String key : _selectKeys) {
				result.put(key, map.get(key));
			}
			return result;
		}
		
		return map;
	}
	
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		where = StringUtils.trimToNull(where);
		_removeKeys = Util.splitColumns(remove);
		_selectKeys = Util.splitColumns(select);
	}
}
