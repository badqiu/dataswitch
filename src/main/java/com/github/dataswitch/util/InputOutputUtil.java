package com.github.dataswitch.util;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
			throw new RuntimeException("open() error,openable:"+openable,e);
		}
	}
	
	public static void flushAll(Flushable... branchs) {
		if(branchs == null) return;
		
		for(Flushable item : branchs) {
			if(item == null) continue;
			
			try {
				item.flush();
			} catch (IOException e) {
				throw new RuntimeException("flush error,Flushable:"+item,e);
			}
		}
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
	
	static ArrayList exitSign = new ArrayList(0);
	public static int asyncCopy(Input input, final Output output,int bufferSize,Processor processor,FailMode failMode, Consumer<Exception> exceptionHandler) {
		return asyncCopy(input,output,bufferSize,processor,failMode,exceptionHandler);
	}
	
	public static int asyncCopy(Input input, final Output output,int bufferSize,Processor processor,Map<String, Object> params, FailMode failMode,Consumer<Exception> exceptionHandler) {
		openAll(params,input, output, processor);
		
		final BlockingQueue<List> queue = new LinkedBlockingQueue(100);
		
		final List<Exception> exceptions = new ArrayList<Exception>();
		
		Thread writeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true) {
						List rows = null;
						try {
							rows = queue.take();
							if(CollectionUtils.isEmpty(rows)) {
								return; //exit sign
							}
							
							
							write(output, rows, processor);
						}catch(InterruptedException e) {
							logger.info("InterruptedException on write thread,exit thread",e);
							return;
						}catch(Exception e) {
							Object firstRow = CollectionUtils.get(rows, 0);
							logger.warn("ignore error on write thread, one dataRow:"+firstRow,e);
							
							collectExceptionIfFailAtEnd(failMode, exceptions, e);
							
							handleException(exceptionHandler, e);
						}
					}
				}finally {
					InputOutputUtil.closeQuietly(output);
				}
			}


		},"asyncCopy_write");
		writeThread.setDaemon(true);
		writeThread.start();
		
		int totalRows = 0;
		try {
			while(true) {
				try {
					List rows = input.read(bufferSize);
					if(CollectionUtils.isEmpty((rows))) {
						break;
					}
					totalRows += rows.size();
					queue.put(rows);
					
					
					input.commitInput();
				}catch(Exception e) {
					String msg = "read error,input:"+input+" output:"+output+" processor:"+processor;
					logger.warn(msg,e);
					if(FailMode.FAIL_FAST == failMode) {
						throw new RuntimeException(msg,e);
					}
					
					collectExceptionIfFailAtEnd(failMode, exceptions, e);
					
					handleException(exceptionHandler, e);
				}
			}
			
			return totalRows;
		}finally {
			try {
				queue.put(exitSign);//exit sign
				writeThread.join();
			}catch(Exception e) {
				throw new RuntimeException(e);
			}
			
			closeAllQuietly(input, output, processor);
			
			if(!exceptions.isEmpty() && FailMode.FAIL_AT_END ==failMode) {
				throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+" exceptions:"+exceptions);
			}
		}
	}
	
	private static void collectExceptionIfFailAtEnd(FailMode failMode, final List<Exception> exceptions,
			Exception e) {
		if(failMode == FailMode.FAIL_AT_END) {
			if(exceptions.size() < 100) { // limit size for OutOfMemoryError
				exceptions.add(e);
			}
		}
	}
	/**
	 * 异步拷贝数据
	 * @param input
	 * @param output
	 * @param processor
	 */
	
	public static int asyncCopy(Input input, final Output output,int bufferSize,Processor processor,String failMode) {
		return asyncCopy(input,output,bufferSize,processor,FailMode.getRequiredByName(failMode),null);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output) {
		return copy(input,output,DEFAULT_BUFFER_SIZE);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,Processor processor) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,boolean ignoreWriteError) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,DEFAULT_PROCESSOR,ignoreWriteError);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,Processor processor,boolean ignoreWriteError) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor,ignoreWriteError);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,int bufferSize) {
		return copy(input,output,bufferSize,DEFAULT_PROCESSOR,false);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,int bufferSize,Processor processor) {
		return copy(input,output,bufferSize,processor,false);
	}
	
	/**
	 * 
	 * @param input
	 * @param output
	 * @param bufferSize
	 * @param ignoreWriteError
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,int bufferSize,Processor processor,boolean ignoreCopyError) {
		FailMode failMode = FailMode.FAIL_NEVER;
		if(!ignoreCopyError) {
			failMode = FailMode.FAIL_FAST;
		}
		return copy(input,output,bufferSize,processor,null,failMode,null);
	}

	public static long copy(Input input,Output output,int bufferSize,Processor processor,Map params,String failMode) {
		return copy(input,output,bufferSize,processor,params,FailMode.getRequiredByName(failMode),null);
	}

	/**
	 * 
	 * @param input
	 * @param output
	 * @param bufferSize
	 * @param failMode,取值: failFast,failAtEnd,failNever
	 * @return 拷贝的数据量
	 */
	public static long copy(Input input,Output output,int bufferSize,Processor processor,Map params,FailMode failMode,Consumer<Exception> exceptionHandler) {
		if(bufferSize <= 0) throw new IllegalArgumentException("bufferSize > 0 must be true");
		
		openAll(params,input, output, processor);
		
		List<Exception> exceptions = new ArrayList<Exception>();
		long count = 0;
		List<Object> rows = null;
		try {
			
			while(true) {
				try {
					rows = input.read(bufferSize);
					if(CollectionUtils.isEmpty((rows))) {
						break;
					}
					
					count += write(output, rows,processor);
					
					input.commitInput();
				}catch(Exception e) {
					Object firstRow = CollectionUtils.get(rows, 0);
					if(FailMode.FAIL_FAST == failMode) {
						throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+", one rowData:"+firstRow,e);
					}
					
					logger.warn("copy warn,input:"+input+" output:"+output+" processor:"+processor+" one rowData:"+firstRow,e);
					
					collectExceptionIfFailAtEnd(failMode, exceptions, e);
					
					handleException(exceptionHandler, e);
				}
			}
		}finally {
			closeAllQuietly(input, output, processor);
		}
		
		if(!exceptions.isEmpty() && FailMode.FAIL_AT_END == failMode) {
			throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+" exceptions:"+exceptions);
		}
		return count;
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

	
}
