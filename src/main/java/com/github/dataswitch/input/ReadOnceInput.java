package com.github.dataswitch.input;

import java.util.List;

import com.github.dataswitch.BaseObject;

/**
 * 只读一次的Input 
 * 
 * @author badqiu
 *
 */
public abstract class ReadOnceInput extends BaseObject implements Input {

	private boolean read = false;

	public List<Object> read(int size) {
		if(read) return null;
		
		synchronized (this) {
			if(read) return null;
			read = true;
		}
		
		try {
			return readOnce(size);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public abstract List<Object> readOnce(int size) throws Exception;

}
