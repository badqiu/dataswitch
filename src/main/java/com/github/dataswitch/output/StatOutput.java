package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.Util;

/**
 * 统计
 * @author badqiu
 *
 */
public class StatOutput extends ProxyOutput {
	private static Logger logger = LoggerFactory.getLogger(StatOutput.class);
	
	private long totalRows;
	private long errorRows;
	private long totalCostTime;
	private boolean printLog;
	
	public StatOutput() {
		super();
	}

	public StatOutput(Output proxy) {
		super(proxy);
	}
	
	public long getTotalRows() {
		return totalRows;
	}

	public long getTotalCostTime() {
		return totalCostTime;
	}
	
	public long getErrorRows() {
		return errorRows;
	}

	public long getTps() {
		return Util.getTPS(totalRows, totalCostTime);
	}
	
	public double getErrorRate() {
		return (double)errorRows / totalRows;
	}
	
	public void setPrintLog(boolean printLog) {
		this.printLog = printLog;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		int rowsSize = rows.size();
		long start = System.currentTimeMillis();
		
		try {
			super.write(rows);
		}catch(RuntimeException e) {
			errorRows += rowsSize;
			throw e;
		}finally {
			long costTimeMills = System.currentTimeMillis() - start;
	
			
			totalRows += rowsSize;
			totalCostTime += costTimeMills;
			
			printLogIfTrue(rowsSize, costTimeMills);
		}
	}

	private void printLogIfTrue(long rowsSize, long costTimeMills) {
		if(printLog) {
			logger.info(getId() + " write() costTimeMills:"+costTimeMills+" rows.size:"+rowsSize+" tps:"+Util.getTPS(rowsSize, costTimeMills));
		}
	}
	
	@Override
	public void close() throws Exception {
		if(logger.isInfoEnabled()) {
			logger.info("stat for write() totalCostTime:"+totalCostTime+" totalRows:"+totalRows+" tps:"+getTps()+" errorRows:"+errorRows);
		}
		super.close();
	}
	
}
