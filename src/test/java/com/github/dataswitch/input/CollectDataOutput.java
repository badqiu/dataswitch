package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.github.dataswitch.output.Output;

public class CollectDataOutput implements Output{

	private List<Object> datas = new ArrayList<Object>();
	
	public List<Object> getDatas() {
		return datas;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void write(List<Object> rows) {
		datas.addAll(rows);
	}

	
}
