package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

public class FileTypeUtil {

	private static final String XML_REGEX = "(?im)<!--(.*?)-->";
	
	private static final String C_REGEX = "(?im)/\\*(.*?)\\*/";
	private static final String C_LINE_REGEX = "(?im)//(.*)";
	
	private static final String SQL_REGEX = C_REGEX;
	private static final String SQL_LINE_REGEX = "(?im)--(.*)";
	
	private static final String PROPERTIES_REGEX = "(?im)#(.*)";
	
	

	public static List<String> getAllComments(String filename,String fileContent) {
		if(StringUtils.isBlank(fileContent)) return Collections.EMPTY_LIST;
		if(StringUtils.isBlank(filename)) return Collections.EMPTY_LIST;
		
		
		String ext = FilenameUtils.getExtension(filename).toLowerCase();
		
		if(isXml(ext)) {
			return findAllByRegexGroup(fileContent, 1,XML_REGEX);
		}else if(isSql(ext)) {
			return findAllByRegexGroup(fileContent, 1,SQL_REGEX,SQL_LINE_REGEX);
		}else if(isPython(ext)) {
			return findAllByRegexGroup(fileContent, 1,PROPERTIES_REGEX);
		}else if(isClike(ext)) {
			return findAllByRegexGroup(fileContent, 1,C_REGEX,C_LINE_REGEX);
		}else if(isPropertiesLike(ext)) {
			return findAllByRegexGroup(fileContent, 1,PROPERTIES_REGEX);
		}else {
			return Collections.EMPTY_LIST;
		}
	}
	
	private static List<String> findAllByRegexGroup(String input,int group,String... regexList) {
		List<String> all = new ArrayList();
		for(String regex : regexList) {
			List<String> list = RegexUtil.findAllByRegexGroup(input, regex, group);
			all.addAll(list);
		}
		return all;
	}

	private static boolean isPython(String ext) {
		return "py".equals(ext);
	}

	private static boolean isPropertiesLike(String ext) {
		return "properties".equals(ext) || "yaml".equals(ext) || "toml".equals(ext) || "sh".equals(ext) || "ini".equals(ext);
	}

	private static boolean isClike(String ext) {
		return "js".equals(ext) || "ts".equals(ext) || "java".equals(ext) || "c".equals(ext) || "cpp".equals(ext);
	}

	private static boolean isSql(String ext) {
		return "sql".equals(ext);
	}

	private static boolean isXml(String ext) {
		return "xml".equals(ext) || "html".equals(ext) || "htm".equals(ext);
	}
	
}
