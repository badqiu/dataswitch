package com.github.dataswitch.util;

import java.io.EOFException;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.Openable;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.processor.DefaultProcessor;
import com.github.dataswitch.processor.Processor;

public class InputOutputUtil {

	private static Logger logger = LoggerFactory.getLogger(InputOutputUtil.class);
	
	private static int DEFAULT_BUFFER_SIZE = 3000;
	private static DefaultProcessor DEFAULT_PROCESSOR = new DefaultProcessor();
	
	public static void flush(Flushable io) {
		try {
			if(io != null) 
				io.flush();
		}catch(Exception e) {
			throw new RuntimeException("flush error",e);
		}
	}
	
	public static void close(AutoCloseable io) {
		try {
			if(io != null) 
				io.close();
		}catch(Exception e) {
			throw new RuntimeException("close error",e);
		}
	}
	
	public static void closeAll(AutoCloseable... items) {
		for(AutoCloseable item : items) {
			close(item);
		}
	}
	
	
	public static void closeAll(FailMode failMode,AutoCloseable... items) {
		failMode.forEach(items, item -> {
			close(item);
		});
	}
	
	public static void closeQuietly(AutoCloseable io) {
		try {
			if(io != null) 
				io.close();
		}catch(Exception e) {
			//ignore
			logger.warn("close error",e);
		}
	}
	
	public static void closeAllQuietly(List<? extends AutoCloseable> closeList) {
		if(closeList == null) return;
		
		for(AutoCloseable item : closeList) {
			closeQuietly(item);
		}
	}
	
	public static void closeAllQuietly(AutoCloseable... closeList) {
		if(closeList == null) return;
		
		for(AutoCloseable item : closeList) {
			closeQuietly(item);
		}
	}

	public static void openAll(Openable... openList) {
		openAll(null,openList);
	}
	
	public static void openAll(FailMode failMode,Map params,Openable... items) {
		failMode.forEach(items, item -> {
			open(params,item);
		});
	}
	
	public static void openAll(Map params,List<? extends Openable> openList) {
		if(openList == null) return;
		
		if(params == null) {
			params = Collections.EMPTY_MAP;
		}
		
		for(Openable item : openList) {
			open(params, item);
		}
		
	}
	
	public static void openAll(Map params,Openable... openList) {
		if(openList == null) return;
		
		if(params == null) {
			params = Collections.EMPTY_MAP;
		}
		
		for(Openable item : openList) {
			open(params, item);
		}
		
	}

	public static void open(Map params, Openable openable) {
		if(openable == null) return;
		
		if(params == null) {
			params = Collections.EMPTY_MAP;
		}
		
		try {
			openable.open(params);
		}catch(Exception e) {
			throw new RuntimeException("open() error,openable:"+openable+" params:"+params,e);
		}
	}
	
	public static void flushAll(Flushable... items) {
		if(items == null) return;
		
		for(Flushable item : items) {
			flush(item);
		}
	}
	
	public static void flushAll(FailMode failMode,Flushable... items) {
		failMode.forEach(items, item -> {
			flush(item);
		});
	}
	
	/**
	 * 将input全部读完,返回所有数据行
	 * @param input
	 * @param bufferSize 每次读的批量大小
	 * @return
	 */
	public static List<Object> readFully(Input input,int bufferSize) {
		List<Object> result = new ArrayList<Object>(bufferSize);
		List<Object> rows = null;
		while(CollectionUtils.isNotEmpty((rows = input.read(bufferSize)))) {
			result.addAll(rows);
		}
		return result;
	}
	
	
//	public static void collectExceptionIfFailAtEnd(FailMode failMode, final List<Exception> exceptions,Exception e) {
//		if(exceptions == null) return;
//		if(e == null) return;
//		
//		if(failMode == FailMode.FAIL_AT_END) {
//			if(exceptions.size() < 100) { // limit size for OutOfMemoryError
//				exceptions.add(e);
//			}
//		}
//	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output) {
		return copy(input,output,DEFAULT_BUFFER_SIZE);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,Processor processor) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,FailMode failMode) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,DEFAULT_PROCESSOR,failMode);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,Processor processor,FailMode failMode) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor,failMode);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,int bufferSize) {
		return copy(input,output,bufferSize,DEFAULT_PROCESSOR,FailMode.FAIL_FAST);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,int bufferSize,Processor processor) {
		return copy(input,output,bufferSize,processor,FailMode.FAIL_FAST);
	}
	
	/**
	 * 
	 * @param input
	 * @param output
	 * @param bufferSize
	 * @param ignoreWriteError
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,int bufferSize,Processor processor,FailMode failMode) {
		return copy(input,output,bufferSize,processor,null,failMode,null);
	}

	public static CopyResult copy(Input input,Output output,int bufferSize,Processor processor,Map params,FailMode failMode) {
		return copy(input,output,bufferSize,processor,params,failMode,null);
	}

	/**
	 * 
	 * @param input
	 * @param output
	 * @param bufferSize
	 * @param failMode,取值: failFast,failAtEnd,failNever
	 * @return 拷贝的数据量
	 */
	public static CopyResult copy(Input input,Output output,int bufferSize,Processor processor,Map params,FailMode failMode,Consumer<Exception> exceptionHandler) {
		if(bufferSize <= 0) throw new IllegalArgumentException("bufferSize > 0 must be true");
		
		long totalCostTime = 0;
		long startTime = System.currentTimeMillis();

		openAll(params,input, output, processor);
		
		Object lastExceptionData = null;
		Exception lastException = null;
		
		long count = 0;
		List<Object> rows = null;
		long readCostSum = 0;
		long writeCostSum = 0;
		try {
			
			while(true) {
				try {
					long startReadTime = System.currentTimeMillis();
					rows = input.read(bufferSize);
					long readCost = System.currentTimeMillis() - startReadTime;
					readCostSum += readCost;
					
					if(CollectionUtils.isEmpty((rows))) {
						break;
					}
					
					long startWriteTime = System.currentTimeMillis();
					count += write(output, rows,processor);
					long writeCost = System.currentTimeMillis() - startWriteTime;
					writeCostSum += writeCost;
					
					input.commitInput();
				}catch(EOFException e) {
					break;
				}catch(Exception e) {
					lastException = e;
					
					Object firstRow = Util.first(rows);
					lastExceptionData = firstRow;
					
					if(FailMode.FAIL_FAST == failMode) {
						throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+", one rowData:"+firstRow,e);
					}
					
					
					logger.warn("copy warn,input:"+input+" output:"+output+" processor:"+processor+" one rowData:"+firstRow,e);
					
					handleException(exceptionHandler, e);
				}
			}
		}finally {
			closeAllQuietly(input, output, processor);
			totalCostTime = System.currentTimeMillis() - startTime; 
		}
		
		CopyResult copyResult = new CopyResult(count,totalCostTime,readCostSum,writeCostSum);
		
		logger.info("copy() result stat:"+copyResult);
		
		if(lastException != null && FailMode.FAIL_AT_END == failMode) {
			throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+" lastExceptionData:"+lastExceptionData + " exception:"+lastException);
		}
		return copyResult;
	}


	private static void handleException(Consumer<Exception> exceptionHandler, Exception e) {
		if(exceptionHandler != null) {
			exceptionHandler.accept(e);
		}
	}
	
	/**
	 * write数据
	 * @return 写的数据量
	 */
	public static int write(Output output, List<Object> rows,Processor processor) throws Exception {
		if(CollectionUtils.isEmpty((rows))) {
			return 0;
		}
		
		List<Object> processedRows = processor == null ? rows : processor.process(rows);
		if(CollectionUtils.isEmpty(processedRows)) {
			return 0;
		}
		
		output.write(processedRows);
		return processedRows.size();
	}
	
	/** copy() 方法的统计结果  */
	public static class CopyResult {
		private long count; //总条数
		private long totalCostTime; //全部总耗时
		private long readCostTime; //读取总耗时
		private long writeCostTime; //写入总耗时
		
		public CopyResult() {
		}
		
		public CopyResult(long count, long totalCostTime,long readCostTime, long writeCostTime) {
			this.count = count;
			this.totalCostTime = totalCostTime;
			this.readCostTime = readCostTime;
			this.writeCostTime = writeCostTime;
		}
		
		public long getCount() {
			return count;
		}
		public void setCount(long count) {
			this.count = count;
		}
		public long getReadCostTime() {
			return readCostTime;
		}
		public void setReadCostTime(long readCostTime) {
			this.readCostTime = readCostTime;
		}
		public long getWriteCostTime() {
			return writeCostTime;
		}
		public void setWriteCostTime(long writeCostTime) {
			this.writeCostTime = writeCostTime;
		}
		public long getTotalCostTime() {
			return totalCostTime;
		}
		public void setTotalCostTime(long totalCostTime) {
			this.totalCostTime = totalCostTime;
		}

		public long getReadTps() {
			return Util.getTPS(count, readCostTime);
		}
		
		public long getWriteTps() {
			return Util.getTPS(count, writeCostTime);
		}
		
		public long getTotalTps() {
			return Util.getTPS(count, totalCostTime);
		}

		@Override
		public String toString() {
			return "CopyResult [count=" + count + ", totalCostTime=" + totalCostTime + ", readCostTime="
					+ readCostTime + ", writeCostTime=" + writeCostTime + ", readTps=" + getReadTps()
					+ ", writeTps=" + getWriteTps() + ", totalTps=" + getTotalTps() + "]";
		}
		
		
	}

	
}
