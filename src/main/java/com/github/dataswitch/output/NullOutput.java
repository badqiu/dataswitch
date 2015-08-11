package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
/**
 * 不做任何事情的Output
 * @author badqiu
 *
 */
public class NullOutput implements Output{

	public void close() throws IOException {
	}

	public void write(List<Object> rows) {
	}

}
