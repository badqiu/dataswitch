package com.github.dataswitch.input;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.Util;

/**
 * 统计
 * @author badqiu
 *
 */
public class StatInput extends ProxyInput {
	private static Logger logger = LoggerFactory.getLogger(StatInput.class);
	private long totalRows;
	private long totalCostTime;
	private boolean printLog;
	
	public StatInput() {
		super();
	}

	public StatInput(Input proxy) {
		super(proxy);
	}
	
	public long getTotalRows() {
		return totalRows;
	}

	public long getTotalCostTime() {
		return totalCostTime;
	}

	public long getTps() {
		return Util.getTPS(totalRows, totalCostTime);
	}
	
	public void setPrintLog(boolean printLog) {
		this.printLog = printLog;
	}

	@Override
	public List<Object> read(int size) {
		
		long start = System.currentTimeMillis();
		List<Object> rows = super.read(size);
		long costTimeMills = System.currentTimeMillis() - start;
		
		int rowsSize = CollectionUtils.isEmpty(rows) ? 0 : rows.size();
		
		totalRows += rowsSize;
		totalCostTime += costTimeMills;
		
		printLogIfTrue(rowsSize, costTimeMills);
		return rows;
	}

	private void printLogIfTrue(long rowsSize, long costTimeMills) {
		if(printLog) {
			logger.info(getId() + " read() costTimeMills:"+costTimeMills+" rows.size:"+rowsSize+" tps:"+Util.getTPS(rowsSize, costTimeMills));
		}
	}
	
	@Override
	public void close() throws Exception {
		logger.info("stat for write() totalCostTime:"+totalCostTime+" totalRows:"+totalRows+" tps:"+getTps());
		super.close();
	}
	
}
