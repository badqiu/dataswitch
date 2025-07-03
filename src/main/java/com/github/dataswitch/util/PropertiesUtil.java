package com.github.dataswitch.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class PropertiesUtil {

    // 从命令行解释参数，参数格式-DtaskId=someId -Ddate="2015-10-10 10:10:10"
    public static Properties parsePropertiesFromCmdLine(String cmdLine) {
        Properties props = new Properties();
        if (StringUtils.isBlank(cmdLine)) {
            return props;
        }

        // 正则表达式匹配 -Dkey=value 或 -Dkey="value with spaces" 格式
        Pattern pattern = Pattern.compile("-D(\\w+)=([^\\s\"]+|\"[^\"]*\")");
        Matcher matcher = pattern.matcher(cmdLine);

        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            
            // 去除值两边的引号（如果有）
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            
            props.setProperty(key, value);
        }

        return props;
    }
	
	public static Properties createProperties(String content)  {
		Properties custom = new Properties();
		if(StringUtils.isNotBlank(content)) {
			try {
				smartLoadFromString(content, custom);
				return trim(custom);
			}catch(Exception e) {
				throw new RuntimeException("error on content:"+content,e);
			}
		}
		
		return custom;
	}

	private static void smartLoadFromString(String content, Properties custom) throws IOException, InvalidPropertiesFormatException {
		if(content.contains("-D")) {
			Properties props = parsePropertiesFromCmdLine(content);
			custom.putAll(props);
			return;
		}
		
		if(content.contains("<properties>") && content.contains("</properties>")) {
			custom.loadFromXML(new ByteArrayInputStream(content.getBytes()));
		}else {
			custom.load(new StringReader(content));
		}
	}

	private static Properties trim(Properties custom) {
		Properties r = new Properties();
		custom.forEach((k,v) -> {
			r.put(trim(k), trim(v));
		});
		return r;
	}

	private static String trim(Object k) {
		if(k == null) return null;
		
		return StringUtils.trim(k.toString());
	}
	
}
