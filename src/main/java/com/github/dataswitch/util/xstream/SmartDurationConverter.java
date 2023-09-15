package com.github.dataswitch.util.xstream;

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;

import com.thoughtworks.xstream.converters.time.DurationConverter;

public class SmartDurationConverter extends DurationConverter {

	@Override
	public Duration fromString(String str) {
		if(StringUtils.isBlank(str)) return null;
		
		if(str.toLowerCase().startsWith("p")) {
			return super.fromString(str);
		}else {
			return super.fromString("PT"+str);
		}
	}
}
