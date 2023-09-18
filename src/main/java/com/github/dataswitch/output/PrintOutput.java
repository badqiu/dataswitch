package com.github.dataswitch.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Objects;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.InputOutputUtil;

public class PrintOutput extends BaseObject implements Output{

	private String prefix = "";
	private PrintStream out = System.out;
	
	private boolean closeOutOnClose = false;
	
	public PrintOutput() {
	}
	
	public PrintOutput(String prefix) {
		this.prefix = prefix;
	}
	
	public PrintOutput(OutputStream out, String prefix) {
		setOut(out);
		this.prefix = prefix;
	}
	
	public PrintOutput(PrintStream out) {
		this.out = out;
	}

	public OutputStream getOut() {
		return out;
	}

	public void setOut(OutputStream out) {
		Objects.requireNonNull(out, "out must be not null");
		this.out = new PrintStream(out);
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	public void setOut(PrintStream out) {
		this.out = out;
	}
	
	public boolean isCloseOutOnClose() {
		return closeOutOnClose;
	}

	public void setCloseOutOnClose(boolean closeOutOnClose) {
		this.closeOutOnClose = closeOutOnClose;
	}

	@Override
	public void write(List<Object> rows) {
		if(rows == null) return;
		
		for(Object row : rows) {
			String string = prefix + row;
			out.println(string);
		}
		
	}
	
	@Override
	public void flush() throws IOException {
		InputOutputUtil.flush(out);
	}
	
	@Override
	public void close() throws IOException {
		if(closeOutOnClose) {
			InputOutputUtil.close(out);
		}
	}

}
