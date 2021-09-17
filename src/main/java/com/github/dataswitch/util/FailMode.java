package com.github.dataswitch.util;

import org.apache.commons.lang.StringUtils;

public enum FailMode {
	FAIL_FAST("failFast"),FAIL_AT_END("failAtEnd"),FAIL_NEVER("failNever");
	

	private final String shortName;
	
	FailMode(String shortName) {
		this.shortName = shortName;
	}

	public String getShortName() {
		return shortName;
	}
	
	public static FailMode getRequiredByName(String name) {
		name = StringUtils.trim(name);
		
		for(FailMode m : values()) {
			if(m.getShortName().equalsIgnoreCase(name)) {
				return m;
			}
			if(m.name().equalsIgnoreCase(name)) {
				return m;
			}
		}
		throw new RuntimeException("not found failMode by name:"+name);
	}
}
