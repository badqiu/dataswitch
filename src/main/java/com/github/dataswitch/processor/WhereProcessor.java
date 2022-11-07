package com.github.dataswitch.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;

import com.github.dataswitch.util.Util;

/**
 * 提供类似SQL的where,select等过滤数据功能
 * 
 * @author badqiu
 *
 */
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
	private long limit; //done
	private long offset; //done
	
//	private String orderBy;
//	private String groupBy;
//	private String having;
	
	
	private String[] _removeKeys = null;
	private String[] _selectKeys = null;
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
				Object value = map.get(key);
				
				if(value != null) {
					result.put(key, value);
				}
			}
			return result;
		}
		
		return map;
	}
	
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
//		where = convertSqlWhere2JavaWhere(where);
		_removeKeys = Util.splitColumns(remove);
		_selectKeys = Util.splitColumns(select);
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
