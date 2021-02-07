package com.github.dataswitch.output;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

public interface Output extends Closeable,Flushable{

	public void write(List<Object> rows);
	
	public default void flush() throws IOException{
	}
	
}

