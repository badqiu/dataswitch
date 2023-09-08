package com.github.dataswitch.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private String select; //要select的列,没有则选择所有列
	private String remove; //要删除的列,没有不移除列
	private long limit; //
	private long offset; //从0开始
	
//	private String orderBy;
//	private String groupBy;
//	private String having;
	private String print; //条件成立时打印日志
	
	
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

	private Object selectByKeys(Map map,String[] keys) {
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
