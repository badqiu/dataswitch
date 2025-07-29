package com.github.dataswitch.util;

import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;

public class UrlUtil {

    public static String httpGet(String url) {
    	InputStream input = null;
    	try {
	    	URL urlObj = new URL(url);
	    	input = urlObj.openStream();
	    	String r = IOUtils.toString(input);
			return r;
    	}catch(Exception e) {
    		throw new RuntimeException(e);
    	}finally {
    		IOUtils.closeQuietly(input);
    	}
	}
    
}
