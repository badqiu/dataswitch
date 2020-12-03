package com.github.dataswitch.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.processor.DefaultProcessor;
import com.github.dataswitch.processor.Processor;

public class InputOutputUtil {

	private static Logger logger = LoggerFactory.getLogger(InputOutputUtil.class);
	
	private static int DEFAULT_BUFFER_SIZE = 3000;
	private static DefaultProcessor DEFAULT_PROCESSOR = new DefaultProcessor();
	
	public static String FAIL_FAST = "failFast";
	public static String FAIL_AT_END = "failAtEnd";
	public static String FAIL_NEVER = "failNever";
	
	public static void close(Closeable io) {
		try {
			if(io != null) 
				io.close();
		}catch(Exception e) {
			throw new RuntimeException("close error",e);
		}
	}
	
	public static void closeQuietly(Closeable io) {
		try {
			if(io != null) 
				io.close();
		}catch(Exception e) {
			//ignore
			logger.warn("close error",e);
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
	
	/**
	 * 异步拷贝数据
	 * @param input
	 * @param output
	 * @param processor
	 */
	static ArrayList exitSign = new ArrayList(0);
	public static int asyncCopy(Input input, final Output output,int bufferSize,Processor processor,String failMode) {
		final BlockingQueue<List> queue = new LinkedBlockingQueue(100);
		
		final List<Exception> exceptions = new ArrayList<Exception>();
		
		Thread writeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true) {
						try {
							List rows = queue.take();
							if(CollectionUtils.isEmpty(rows)) {
								return; //exit sign
							}
							output.write(rows);
						}catch(InterruptedException e) {
							logger.info("InterruptedException on write thread,exit thread",e);
							return;
						}catch(Exception e) {
							exceptions.add(e);
							logger.warn("ignore error on write thread",e);
						}
					}
				}finally {
					IOUtils.closeQuietly(output);
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
				}catch(Exception e) {
					String msg = "read error,input:"+input+" output:"+output+" processor:"+processor;
					logger.warn(msg,e);
					if(FAIL_FAST.equals(failMode)) {
						throw new RuntimeException(msg,e);
					}
					exceptions.add(e);
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
			
			IOUtils.closeQuietly(input);
			
			if(!exceptions.isEmpty() && FAIL_AT_END.equals(failMode)) {
				throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+" exceptions:"+exceptions);
			}
		}
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output) {
		return copy(input,output,DEFAULT_BUFFER_SIZE);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,Processor processor) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,boolean ignoreWriteError) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,DEFAULT_PROCESSOR,ignoreWriteError);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,Processor processor,boolean ignoreWriteError) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,processor,ignoreWriteError);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,int bufferSize) {
		return copy(input,output,bufferSize,DEFAULT_PROCESSOR,false);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,int bufferSize,Processor processor) {
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
	public static int copy(Input input,Output output,int bufferSize,Processor processor,boolean ignoreCopyError) {
		String failMode = FAIL_NEVER;
		if(!ignoreCopyError) {
			failMode = FAIL_FAST;
		}
		return copy(input,output,bufferSize,processor,failMode);
	}

	
	/**
	 * 
	 * @param input
	 * @param output
	 * @param bufferSize
	 * @param failMode,取值: failFast,failAtEnd,failNever
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,int bufferSize,Processor processor,String failMode) {
		if(bufferSize <= 0) throw new IllegalArgumentException("bufferSize > 0 must be true");
		if(!(FAIL_FAST.equals(failMode) || FAIL_AT_END.equals(failMode) || FAIL_NEVER.equals(failMode))) {
			throw new RuntimeException("legal failMode is: "+FAIL_FAST+","+FAIL_AT_END+","+FAIL_NEVER+" current:"+failMode);
		}
		
		List<Object> rows = null;
		int count = 0;
		List<Exception> exceptions = new ArrayList<Exception>();
		
		try {
			while(true) {
				try {
					rows = input.read(bufferSize);
					if(CollectionUtils.isEmpty((rows))) {
						break;
					}
					
					count += write(output, rows,processor);
				}catch(Exception e) {
					if(FAIL_FAST.equals(failMode)) {
						throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor,e);
					}
					logger.warn("copy warn,input:"+input+" output:"+output+" processor:"+processor,e);
					exceptions.add(e);
				}
			}
		}finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(output);
		}
		
		if(!exceptions.isEmpty() && FAIL_AT_END.equals(failMode)) {
			throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor+" exceptions:"+exceptions);
		}
		return count;
	}
	
	/**
	 * write数据
	 * @return 写的数据量
	 */
	public static int write(Output output, List<Object> rows,Processor processor) throws Exception {
		List<Object> processedRows = processor == null ? rows : processor.process(rows);
		if(CollectionUtils.isNotEmpty(processedRows)) {
			output.write(processedRows);
			return processedRows.size();
		}
		return 0;
	}
	
}
