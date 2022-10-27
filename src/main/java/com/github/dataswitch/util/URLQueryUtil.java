package com.github.dataswitch.util;

import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;


public class URLQueryUtil {

	public static Map<String, List<String>> splitQuery(URL url) {
	    String query = url.getQuery();
		return splitQuery(query);
	}

	public static Map<String, List<String>> splitQuery(String query) {
		if (StringUtils.isBlank(query)) {
	        return Collections.emptyMap();
	    }
	    
	    return Arrays.stream(query.split("&"))
	            .map(URLQueryUtil::splitQueryParameter)
	            .collect(Collectors.groupingBy(SimpleImmutableEntry::getKey, LinkedHashMap::new, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
	}
	
	public static Map<String, String> splitQueryForSingleValue(String query) {
		if (StringUtils.isBlank(query)) {
	        return Collections.emptyMap();
	    }
	    
	    List<SimpleImmutableEntry<String, String>> list = Arrays.stream(query.split("&"))
	            .map(URLQueryUtil::splitQueryParameter)
	            .collect(Collectors.toList());
	    Map result = new HashMap();
	    list.forEach((entry) -> {
	    	result.put(entry.getKey(),entry.getValue());
	    });
	    return result;
	}
	

	public static SimpleImmutableEntry<String, String> splitQueryParameter(String it)  {
	    final int idx = it.indexOf("=");
	    final String key = idx > 0 ? it.substring(0, idx) : it;
	    final String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
	    
	    try {
		    return new SimpleImmutableEntry<>(
		        URLDecoder.decode(key, StandardCharsets.UTF_8.toString()),
		        URLDecoder.decode(value, StandardCharsets.UTF_8.toString())
		    );
	    }catch(Exception e) {
	    	throw new RuntimeException(e);
	    }
	}
	
}
