package com.github.dataswitch.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;

public class KafkaConfigUtil {

	public static void initJavaSecurityAuthLoginConfig(Properties properties) {
		String loginConfig = properties.getProperty("java.security.auth.login.config");
		setJavaSecurityAuthLoginConfig(loginConfig);
		
		String useSubjectCredsOnly = properties.getProperty("javax.security.auth.useSubjectCredsOnly");
		if(StringUtils.isNotBlank(useSubjectCredsOnly)) {
			System.setProperty("javax.security.auth.useSubjectCredsOnly", useSubjectCredsOnly);
		}
	}
	
	public static void setJavaSecurityAuthLoginConfig(String classpathFile) {
		if(StringUtils.isBlank(classpathFile)) return;
		if(StringUtils.isNotBlank(System.getProperty("java.security.auth.login.config"))) return;
		
		try {
			File tempFile = saveJaasFile2TempFile(classpathFile);
			System.setProperty("java.security.auth.login.config", tempFile.getAbsolutePath());
		}catch(Exception e) {
			throw new RuntimeException("error on classpathFile:"+classpathFile,e);
		}
	}

	private static File saveJaasFile2TempFile(String path) throws IOException, FileNotFoundException {
		File tempFile = new File(FileUtils.getTempDirectory(), "jaas-" + System.currentTimeMillis() + ".conf");
		InputStream input = new ClassPathResource(path).getInputStream();
		Assert.notNull(input,"not found resource:"+path);
		FileOutputStream output = null;
		try {
			output = new FileOutputStream(tempFile);
			IOUtils.write(IOUtils.toString(input), output);
		} finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(output);
		}
		return tempFile;
	}

}
