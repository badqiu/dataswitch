package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;

import com.github.dataswitch.BaseObject;

public class PrintOutput extends BaseObject implements Output{

	private String prefix = "";
	
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
			System.out.println(prefix + row);
		}
		
	}

}
