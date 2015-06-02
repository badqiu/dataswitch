package com.github.dataswitch.storage;

import java.util.List;

public class Storage {

	public boolean isInputStored(String storegeId) {
		return false;
	}
	
	public boolean saveInputStored(String storegeId) {
		return false;
	}

	public void write(List<Object> rows) {
	}

	public List<Object> read(int readSize) {
		return null;
	}

}
