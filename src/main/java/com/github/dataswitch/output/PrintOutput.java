package com.github.dataswitch.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.github.dataswitch.BaseObject;

public class PrintOutput extends BaseObject implements Output{

	private String prefix = "";
	private OutputStream out = System.out;
	
	public OutputStream getOut() {
		return out;
	}

	public void setOut(OutputStream out) {
		this.out = out;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public void write(List<Object> rows) {
		if(rows == null) return;
		
		for(Object row : rows) {
			String string = prefix + row + "\n";
			try {
				out.write(string.getBytes());
			} catch (IOException e) {
				throw new RuntimeException("write error on line:"+string,e);
			}
		}
		
	}

}
