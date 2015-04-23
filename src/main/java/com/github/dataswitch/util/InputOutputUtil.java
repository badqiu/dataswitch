package com.github.dataswitch.util;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;

public class InputOutputUtil {

	private static int DEFAULT_BUFFER_SIZE = 3000;
	
	public static void close(Closeable io) {
		try {
			if(io != null) 
				io.close();
		}catch(Exception e) {
			//ignore
		}
	}
	
	public static void copy(Input input,Output output) {
		copy(input,output,DEFAULT_BUFFER_SIZE);
	}
	
	public static void copy(Input input,Output output,int readSize) {
		copy(input,output,readSize,false);
	}
	
	public static void copy(Input input,Output output,int readSize,boolean ignoreWriteError) {
		List<Object> rows = null;
		while(!(rows = input.read(readSize)).isEmpty()) {
			try {
				output.write(rows);
			}catch(Exception e) {
				if(ignoreWriteError) {
					continue;
				}
				throw new RuntimeException("copy error",e);
			}
		}
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
