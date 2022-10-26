package com.github.dataswitch;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public interface Enabled {

	public default boolean enabled() {
		return true;
	}
	
	public static void assertEnabled(Object item) {
		if(item == null) return;
		if(item instanceof Enabled) {
			assertEnabled((Enabled)item);
		}
	}
	
	public static void assertEnabled(Enabled item) {
		if(item == null) return;
		
		if(!item.enabled()) {
			throw new IllegalStateException("enabled is false, " + item);
		}
	}
	
	public static <T> T[] filterByEnabled(T... items) {
		if(items == null) return null;
		
		List<T> results = new ArrayList<T>();
		for(T item : items) {
			if(item == null) continue;
			
			if(item instanceof Enabled) {
				Enabled enabled = (Enabled)item;
				if(enabled.enabled()) {
					results.add(item);
				}
			}else {
				results.add(item);
			}
		}
		
		if(results.isEmpty()) return null;
		
		T[] array = (T[])Array.newInstance(items.getClass().getComponentType(), results.size());
		
		return (T[])results.toArray(array);
	}
	
	public static <T> List<T> filterByEnabled(List<T> items) {
		if(items == null) return null;
		
		List<T> results = new ArrayList();
		for(T item : items) {
			if(item == null) continue;
			
			if(item instanceof Enabled) {
				Enabled enabled = (Enabled)item;
				if(enabled.enabled()) {
					results.add(item);
				}
			}else {
				results.add(item);
			}
		}
		
		return results;
	}
	
}
