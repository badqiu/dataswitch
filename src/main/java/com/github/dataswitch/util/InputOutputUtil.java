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
		List<Object> rows = null;
		while(!(rows = input.read(readSize)).isEmpty()) {
			output.write(rows);
		}
	}
	
}
