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

	private boolean readOnce = false;

	public List<Object> read(int size) {
		if(readOnce) return null;
		
		synchronized (this) {
			if(readOnce) return null;
			readOnce = true;
		}
		
		try {
			return readOnce(size);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public abstract List<Object> readOnce(int size) throws Exception;

}
