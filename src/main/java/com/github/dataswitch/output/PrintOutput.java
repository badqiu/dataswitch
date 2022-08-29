package com.github.dataswitch.output;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import com.github.dataswitch.BaseObject;

public class PrintOutput extends BaseObject implements Output{

	private String prefix = "";
	private PrintStream out = System.out;
	
	public OutputStream getOut() {
		return out;
	}

	public void setOut(OutputStream out) {
		this.out = new PrintStream(out);
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
			String string = prefix + row;
			out.println(string);
		}
		
	}

}
