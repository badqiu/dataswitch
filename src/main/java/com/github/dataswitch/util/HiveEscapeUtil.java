package com.github.dataswitch.util;

import org.apache.commons.lang.StringUtils;

public class HiveEscapeUtil {

	public static String hiveEscaped(String str) {
		if(str == null) return null;
		
		String result = StringUtils.replace(str, "\n", "\\n");
//		result = StringUtils.replace(result, "\001", "\\001");
//		result = StringUtils.replace(result, "\002", "\\002");
//		result = StringUtils.replace(result, "\003", "\\003");
		return result;
	}
	
}
