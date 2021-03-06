package com.github.dataswitch.util;

import org.apache.commons.lang.StringUtils;

public class HiveEscapeUtil {

	public static String hiveEscaped(String str) {
		if(str == null) return null;
		
		String result = StringUtils.replace(str, "\n", "\\n");
		

		result = StringUtils.replace(result, "\1", "\\1");
		result = StringUtils.replace(result, "\2", "\\2");
		result = StringUtils.replace(result, "\3", "\\3");
		return result;
	}
	
}
