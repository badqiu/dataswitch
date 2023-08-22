package com.github.dataswitch.input;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.github.dataswitch.output.Output;

public class OutputStreamOutput implements Output{

	private OutputStream output;
	
	public OutputStreamOutput(OutputStream output) {
		super();
		this.output = output;
	}

	@Override
	public void close() throws IOException {
		if(output != null) {
			output.close();
		}
	}

	@Override
	public void write(List<Object> rows){
		try {
			for(Object row : rows) {
				if(row == null) continue;
				
				output.write(row.toString().getBytes());
				output.write("\n".getBytes());
			}
		}catch(Exception e) {
			throw new RuntimeException("write error",e);
		}
	}

}
