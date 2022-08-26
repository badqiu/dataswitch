package com.github.dataswitch.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.util.StringUtils;

public class Util {

	public static String[] splitColumns(String columns) {
		String[] result = StringUtils.trimArrayElements(columns.trim().split("[,\\s]+"));
		return result;
	}
	
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
	
	public static Function toFunction(Supplier supplier) {
		return new Function() {
			public Object apply(Object size) {
				return supplier.get();
			}
		};
	}
	
	public static Function toFunction(Callable callable) {
		return new Function() {
			public Object apply(Object size) {
				try {
					return callable.call();
				} catch (Exception e) {
					throw new RuntimeException("callable error",e);
				}
			}
		};
	}
}
