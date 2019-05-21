package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;

public class PrintOutput implements Output{

	@Override
	public void close() throws IOException {
	}

	@Override
	public void write(List<Object> rows) {
		if(rows == null) return;
		
		for(Object row : rows) {
			System.out.println(row);
		}
		
	}

}
