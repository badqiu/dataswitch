package com.github.dataswitch.util;

import org.springframework.util.StringUtils;

public class Util {

	public static String[] splitColumns(String columns) {
		return StringUtils.trimArrayElements(columns.split(","));
	}
	
}
