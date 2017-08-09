package com.github.dataswitch.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
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
	public static int copy(Input input,Output output,int bufferSize,Processor processor,boolean ignoreWriteError) {
		if(bufferSize <= 0) throw new IllegalArgumentException("readSize > 0 must be true");
		List<Object> rows = null;
		int count = 0;
		while(CollectionUtils.isNotEmpty((rows = input.read(bufferSize)))) {
			try {
				count += write(output, rows,processor);
			}catch(Exception e) {
				if(ignoreWriteError) {
					continue;
				}
				throw new RuntimeException("copy error,input:"+input+" output:"+output+" processor:"+processor,e);
			}
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
