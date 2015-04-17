package com.github.dataswitch.output;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Output extends Closeable{

	public void write(List<Object> rows);
	
}
