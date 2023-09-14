package com.github.dataswitch.input;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;

import com.github.dataswitch.util.ThreadUtil;
/**
 * 生成测试数据的Input
 * 
 * @author badqiu
 *
 */
public class DataGenInput implements Input{

	private int rowsPerSecond = 1000;
	private int intervalSecond = 1;
	private long rowsLimit;

	private long _count = 0;
	private long _systemStartTime = System.currentTimeMillis();
	
	/**
	 * 每秒生成的数据行数
	 */
	public int getRowsPerSecond() {
		return rowsPerSecond;
	}

	public void setRowsPerSecond(int rowsPerSecond) {
		this.rowsPerSecond = rowsPerSecond;
	}

	/**
	 * 限制总生成数据行数
	 */
	public long getRowsLimit() {
		return rowsLimit;
	}


	public void setRowsLimit(long rowsLimit) {
		this.rowsLimit = rowsLimit;
	}

	public int getIntervalSecond() {
		return intervalSecond;
	}

	public void setIntervalSecond(int intervalSecond) {
		this.intervalSecond = intervalSecond;
	}

	@Override
	public List<Object> read(int size) {
		return genRows();
	}


	private List<Object> genRows() {
		if(reachRowsLimit()) {
			return Collections.EMPTY_LIST;
		}
		
		List result = new ArrayList();
		
		for(int i = 0; i < rowsPerSecond; i++) {
			if(reachRowsLimit()) {
				break;
			}
			
 			Object row = genRow(i);
			result.add(row);

			_count++;
		}
		
		ThreadUtil.sleepSeconds(intervalSecond);
		return result;
	}


	private boolean reachRowsLimit() {
		boolean result = rowsLimit > 0 && _count >= rowsLimit;
		return result;
	}


	private Object genRow(int i) {
		long age = 10 + (_count % 20);
		Date birthDate = new Date(_systemStartTime);
		int days = (int)(_count % 10000 + 1);

		Map map = new HashMap();
		map.put("id", _count);
		map.put("group", "group_"+(_count % 10));
		map.put("name", "name_"+(_count % 100));
		map.put("type", "type_"+(_count % 1000));
		map.put("age", (int)age);
		map.put("birthDate", DateUtils.addDays(birthDate,-days));
		map.put("offlineDate", DateUtils.addDays(birthDate,days));
		map.put("money", _systemStartTime + i * 100);
		map.put("pay", _count * 100);
		map.put("fee", (_systemStartTime + i * 50) / 10000.0);
		map.put("createTime", new Timestamp(System.currentTimeMillis()));
		map.put("enabled", _count % 5 == 1 ? false : true);
		
		return map;
	}

}
