package com.github.dataswitch.input;

import java.util.List;

/**
 * 只读一次的Input 
 * 
 * @author badqiu
 *
 */
public abstract class ReadOnceInput implements Input {

	private boolean read = false;

	public List<Object> read(int size) {
		if(read) return null;
		read = true;
		
		try {
			return readInternal(size);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public abstract List<Object> readInternal(int size) throws Exception;

}
