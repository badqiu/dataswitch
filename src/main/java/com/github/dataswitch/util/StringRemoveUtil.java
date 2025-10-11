package com.github.dataswitch.util;

public class StringRemoveUtil {

	// 方法一：手动循环过滤（性能最佳）
	public static String removeControlCharactersLoop(String input) {
	    if (input == null || input.isEmpty()) {
	        return input;
	    }
	    int length = input.length();
	    // 构建一个足够大的数组来存放结果字符
	    char[] resultChars = new char[length];
	    int count = 0;
	    
	    for (int i = 0; i < length; i++) {
	        char c = input.charAt(i);
	        // 只保留不在控制字符范围(\u0000-\u001F)内的字符
	        if (c > '\u001F') {
	            resultChars[count++] = c;
	        }
	    }
	    // 根据实际过滤后的字符数量创建新字符串，避免空间浪费
	    return new String(resultChars, 0, count);
	}

	// 方法二：使用StringBuilder（推荐，性能与可读性平衡）
	public static String removeControlCharactersStringBuilder(String input) {
	    if (input == null || input.isEmpty()) {
	        return input;
	    }
	    StringBuilder sb = new StringBuilder(input.length());
	    for (int i = 0; i < input.length(); i++) {
	        char c = input.charAt(i);
	        if (c > '\u001F') {
	            sb.append(c);
	        }
	    }
	    return sb.toString();
	}
	
}
