package com.github.dataswitch.input;

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

	public List<Object> read(int size) {
		if(_readOnce) return null;
		
		synchronized (this) {
			if(_readOnce) return null;
			_readOnce = true;
		}
		
		try {
			return getProxy().read(size);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
