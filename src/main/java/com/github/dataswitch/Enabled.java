package com.github.dataswitch;

import java.util.ArrayList;
import java.util.List;

public interface Enabled {

	public default boolean enabled() {
		return true;
	}
	
	public static <T> T[] filterByEnabled(T... items) {
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
		
		return (T[])results.toArray(new Object[0]);
	}
	
	public static <T> List<T> filterByEnabled(List<T> items) {
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
