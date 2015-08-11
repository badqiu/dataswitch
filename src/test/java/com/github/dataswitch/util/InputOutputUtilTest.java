package com.github.dataswitch.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;

public class InputOutputUtilTest {
	List<Object> object = new ArrayList();
	@Test
	public void test() {
		for(int i = 0; i < 10; i++) {
			object.add(RandomStringUtils.random(10));
		}
		
		InputOutputUtil.copy(new Input(){
			public void close() throws IOException {
				
			}
			public List<Object> read(int size) {
				if(!object.isEmpty()) {
					return Arrays.asList(object.remove(0));
				}
				return null;
			}
			
		}, new Output() {
			@Override
			public void close() throws IOException {
			}

			@Override
			public void write(List<Object> rows) {
			}
		}, null);
	}

}
