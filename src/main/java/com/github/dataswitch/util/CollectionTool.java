package com.github.dataswitch.util;

import java.util.Arrays;
import java.util.Collection;

public class CollectionTool {
	
	public static Collection oneToList(Object row)  {
		if(row == null) return null;
		
		if(row instanceof Collection) {
			return (Collection)row;
		}else if(row.getClass().isArray()) {
			Object[] array = (Object[])row;
			return Arrays.asList(array);
		}else {
			return Arrays.asList(row);
		}
	}
	
}
