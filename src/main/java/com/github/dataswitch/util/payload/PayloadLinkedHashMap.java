package com.github.dataswitch.util.payload;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.dataswitch.enums.DataKind;

public class PayloadLinkedHashMap<K,V,T> extends HashMap<K,V> implements Payloadable<T>{
	
	private static final long serialVersionUID = 1L;
	
	private T payload;

	public PayloadLinkedHashMap() {
		super();
	}

	public PayloadLinkedHashMap(T rowKind) {
		super();
		setPayload(rowKind);
	}
	
	public PayloadLinkedHashMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public PayloadLinkedHashMap(int initialCapacity) {
		super(initialCapacity);
	}
	
	public PayloadLinkedHashMap(int initialCapacity,T rowKind) {
		super(initialCapacity);
		setPayload(rowKind);
	}
	
	public PayloadLinkedHashMap(Map m) {
		super(m);
	}

	public T getPayload() {
		return payload;
	}

	public void setPayload(T payload) {
		this.payload = payload;
	}
	
	
}
