package com.github.dataswitch.input;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.RandomStringUtils;

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
		
		List result = new ArrayList(rowsPerSecond);
		
		long start = System.currentTimeMillis();
		for(int i = 0; i < rowsPerSecond; i++) {
			if(reachRowsLimit()) {
				break;
			}
			
 			Object row = genRow(i,_count);
			result.add(row);

			_count++;
		}
		long costMills = System.currentTimeMillis() - start;
		
		long sleepMills = intervalSecond * 1000 - costMills;
		ThreadUtil.sleep(sleepMills);
		return result;
	}


	private boolean reachRowsLimit() {
		boolean result = rowsLimit > 0 && _count >= rowsLimit;
		return result;
	}


	protected Object genRow(int i,long count) {
		long age = 10 + (count % 20);
		Date date = new Date(_systemStartTime);
		int days = (int)(count % 10000 + 1);
		long money = _systemStartTime + i * 100;
		String email = "hi"+(_count % 10000)+"@qq.com";
		
		Map map = new HashMap(33);
		map.put("id", count);
		map.put("group", "group_"+(count % 10));
		map.put("name", "name_"+(count % 100));
		map.put("type", "type_"+(count % 1000));
		map.put("age", (int)age);
		map.put("birthDate", DateUtils.addDays(date,-days));
		map.put("offlineDate", DateUtils.addDays(date,days));
		map.put("random1", RandomUtils.nextInt(10));
		map.put("random2", RandomUtils.nextInt(100));
		map.put("random3", RandomUtils.nextInt(1000));
		map.put("money", money);
		map.put("pay", count * 100);
		map.put("fee", (_systemStartTime + i * 50) / 10000.0);
		map.put("createTime", new Timestamp(System.currentTimeMillis()));
		map.put("enabled", count % 5 == 1 ? false : true);
		map.put("email", email);
		map.put("password", RandomStringUtils.randomAlphanumeric(32));
		
		map.put("nullAge", count % 10 == 1 ? null : i);
		map.put("nullEmail", count % 100 == 1 ? null : email);
		map.put("nullMoney", count % 1000 == 1 ? null : money);
		map.put("nullBirthDate", count % 10 == 1 ? null :DateUtils.addDays(date,-days));
		
		return map;
	}

}
