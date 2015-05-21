package com.github.dataswitch.util;

import java.io.Closeable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;

public class InputOutputUtil {

	private static Logger logger = LoggerFactory.getLogger(InputOutputUtil.class);
	
	private static int DEFAULT_BUFFER_SIZE = 3000;
	
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
	public static int copy(Input input,Output output,boolean ignoreWriteError) {
		return copy(input,output,DEFAULT_BUFFER_SIZE,ignoreWriteError);
	}
	
	/**
	 * 拷贝数据
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,int readSize) {
		return copy(input,output,readSize,false);
	}
	/**
	 * 
	 * @param input
	 * @param output
	 * @param readSize
	 * @param ignoreWriteError
	 * @return 拷贝的数据量
	 */
	public static int copy(Input input,Output output,int readSize,boolean ignoreWriteError) {
		List<Object> rows = null;
		int count = 0;
		while(!(rows = input.read(readSize)).isEmpty()) {
			try {
				output.write(rows);
				count += rows.size();
			}catch(Exception e) {
				if(ignoreWriteError) {
					continue;
				}
				throw new RuntimeException("copy error",e);
			}
		}
		return count;
	}
	
	//FIXME copy by storage
	public static void copy(Input input,Output output,int readSize,String storegeId,Storage storage) {
		if(!storage.isInputStored(storegeId)){
			List<Object> rows = null;
			while(!(rows = input.read(readSize)).isEmpty()) {
				storage.write(rows);
			}
			storage.saveInputStored(storegeId);
		}
		List<Object> rows = null;
		while(!(rows = storage.read(readSize)).isEmpty()) {
			output.write(rows);
		}
	}
	
}
