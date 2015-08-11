package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RandomStringInput extends BaseInput implements Input{
	List<Object> objects = new ArrayList();
	
	public RandomStringInput(int count) {
		for(int i = 0; i < count; i++) {
			objects.add(""+i);
		}
	}
	
	@Override
	public Object readObject() {
		if(!objects.isEmpty()) {
			return objects.remove(0);
		}
		return null;
	}

	@Override
	public void close() throws IOException {
	}
		
}
