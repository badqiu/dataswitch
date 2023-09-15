package com.github.dataswitch.util.payload;

import java.util.HashMap;
import java.util.Map;

public class PayloadHashMap<K,V,T> extends HashMap<K,V> implements Payloadable<T>{

	private static final long serialVersionUID = 1L;
	
	private T payload;

	public PayloadHashMap() {
		super();
	}

	public PayloadHashMap(T rowKind) {
		super();
		setPayload(rowKind);
	}
	
	public PayloadHashMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public PayloadHashMap(int initialCapacity) {
		super(initialCapacity);
	}
	
	public PayloadHashMap(int initialCapacity,T rowKind) {
		super(initialCapacity);
		setPayload(rowKind);
	}
	
	public PayloadHashMap(Map m) {
		super(m);
	}

	public T getPayload() {
		return payload;
	}

	public void setPayload(T payload) {
		this.payload = payload;
	}
	
}
