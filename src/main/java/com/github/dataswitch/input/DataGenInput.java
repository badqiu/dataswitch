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
	
	private boolean fast = false; //是否快速生成行数据
	
	public DataGenInput() {
	}
	
	public DataGenInput(long rowsLimit) {
		this.rowsLimit = rowsLimit;
	}
	
	public DataGenInput(long rowsLimit,int rowsPerSecond) {
		this.rowsPerSecond = rowsPerSecond;
		this.rowsLimit = rowsLimit;
	}

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
	
	public boolean isFast() {
		return fast;
	}

	public void setFast(boolean fast) {
		this.fast = fast;
	}

	@Override
	public List<Map<String, Object>> read(int size) {
		return genRows();
	}


	private List<Map<String, Object>> genRows() {
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
		Date date = new Date(_systemStartTime);
		int days = (int)(count % 10000 + 1);
		long num10 = count % 10;
		long num100 = count % 100;
		long num1000 = count % 1000;

		int age = (int)(10 + (count % 20));
		long money = _systemStartTime + i * 100;
		float pay = count * 100;
		double fee = (_systemStartTime + i * 50) / 10000.0;

		boolean enabled = count % 5 == 1 ? false : true;
		
		String email = "hi"+(_count % 10000)+"@qq.com";
		
		Map map = new HashMap(40);
		map.put("id", count);
		
		//number
		map.put("age", age);
		map.put("money", money);
		map.put("pay", pay);
		map.put("fee", fee);
		
		//boolean
		map.put("enabled", enabled);
		
		map.put("nullAge", num10 == 1 ? null : i);
		map.put("nullEmail", num100 == 1 ? null : email);
		map.put("nullMoney", num1000 == 1 ? null : money);

		if(fast) {
			return map;
		}
		
		//date 有性能影响
		map.put("nullBirthDate", num10 == 1 ? null :DateUtils.addDays(date,-days));
		map.put("birthDate", DateUtils.addDays(date,-days));
		map.put("offlineDate", DateUtils.addDays(date,days));
		map.put("createTime", new Timestamp(System.currentTimeMillis()));
		
		//random 有性能影响
		map.put("random1", RandomUtils.nextInt(10));
		map.put("random2", RandomUtils.nextInt(100));
		map.put("random3", RandomUtils.nextInt(1000));
		map.put("password", RandomStringUtils.randomAlphanumeric(8));
		
		//string 有性能影响
		
		map.put("group", "group_"+num10);
		map.put("name", "name_"+num100);
		map.put("type", "type_"+num1000);
		map.put("email", email);
		
		return map;
	}

}
