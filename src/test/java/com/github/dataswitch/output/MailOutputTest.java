package com.github.dataswitch.output;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.springframework.mail.javamail.JavaMailSenderImpl;

public class MailOutputTest {

	MailOutput output = new MailOutput();
	@Test
	public void testClose() throws IOException {
		JavaMailSenderImpl sender = buildMailSender();
		
		output.setJavaMailSender(sender);
		output.setSubject("subject from MailOutputTest");
		output.setContentTemplate("<h1>FROM</h1> <#list rows as row> hi ${row} </#list>");
		output.setTo("badqiu@qq.com");
		output.setFrom("xgsdkdata@126.com");
		
		List rows = Arrays.asList("badqiu","jane","bruce","lee");
		output.write(rows);
		
		output.close();
	}
	
	private JavaMailSenderImpl buildMailSender() {
		JavaMailSenderImpl sender = new JavaMailSenderImpl();
		sender.setHost("smtp.126.com");
//		sender.setPort(587);
		sender.setUsername("xgsdkdata@126.com");
		sender.setPassword("nxusvgumljntydoq");
		
		
		Properties javaMailProperties = new Properties();
		javaMailProperties.setProperty("mail.smtp.auth", "true");
		javaMailProperties.setProperty("mail.smtp.debug", "true");
		sender.setJavaMailProperties(javaMailProperties);
		return sender;
	}

}
