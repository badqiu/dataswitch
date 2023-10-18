package com.github.dataswitch.input;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NullInput implements Input{

	public void close() throws IOException {
	}

	public List<Map<String, Object>> read(int size) {
		return Collections.EMPTY_LIST;
	}

}
