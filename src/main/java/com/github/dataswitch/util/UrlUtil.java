package com.github.dataswitch.util;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;

public class UrlUtil {

	static int timeoutSeconds = 10 * 1000;
	
    public static String httpGet(String url) {
    	InputStream input = null;
    	try {
	    	URL urlObj = new URL(url);
	    	URLConnection conn = urlObj.openConnection();
			conn.setConnectTimeout(timeoutSeconds);
	    	conn.setReadTimeout(timeoutSeconds);
	    	input = conn.getInputStream();
	    	String r = IOUtils.toString(input);
			return r;
    	}catch(Exception e) {
    		throw new RuntimeException(e);
    	}finally {
    		IOUtils.closeQuietly(input);
    	}
	}
    
}
