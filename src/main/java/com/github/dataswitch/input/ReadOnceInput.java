package com.github.dataswitch.input;

import java.util.Collections;
import java.util.List;

/**
 * 只读一次的Input 
 * 
 * @author badqiu
 *
 */
public class ReadOnceInput extends ProxyInput implements Input {

	private boolean _readOnce = false;
	
	public ReadOnceInput() {
		super();
	}

	public ReadOnceInput(Input proxy) {
		super(proxy);
	}

	public synchronized List<Object> read(int size) {
		if(_readOnce) return Collections.EMPTY_LIST;
		
		if(_readOnce) return Collections.EMPTY_LIST;
		_readOnce = true;
		
		try {
			return getProxy().read(size);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
