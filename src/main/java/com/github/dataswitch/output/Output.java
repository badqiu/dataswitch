package com.github.dataswitch.output;

import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.Openable;

public interface Output extends Function<List<Object>,Void>,Consumer<List<Object>>,AutoCloseable,Flushable,Openable{

	public void write(List<Object> rows);
	
	@Override
	public default void flush() throws IOException{
	}
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
		Enabled.assertEnabled(this);
	}
	
	@Override
	public default void close() throws Exception {
		flush();
	}
	
	@Override
	default void accept(List<Object> t) {
		write(t);
	}
	
	@Override
	default Void apply(List<Object> t) {
		accept(t);
		return null;
	}
	
}

