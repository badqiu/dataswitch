package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.StringUtils;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class MailOutput implements Output{
	private JavaMailSender javaMailSender;
	
	private List<Object> rows = new ArrayList();
	
	private String from;
	private String[] to;
	private String[] cc;
	private String[] bcc;
	private String replyTo;
	private String subject;
	private String contentHead;
	private String contentFooter;
	private String contentTemplate;
	
	private Configuration freemarkerConf;
	private int maxRows = 0;
	
	public void setTo(String... to) {
		this.to = to;
	}
	
	public String[] getTo() {
		return to;
	}
	
	public String[] getCc() {
		return cc;
	}
	
	public void setCc(String... cc) {
		this.cc = cc;
	}

	public String[] getBcc() {
		return bcc;
	}

	public void setBcc(String... bcc) {
		this.bcc = bcc;
	}
	
	public JavaMailSender getJavaMailSender() {
		return javaMailSender;
	}

	public void setJavaMailSender(JavaMailSender javaMailSender) {
		this.javaMailSender = javaMailSender;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getReplyTo() {
		return replyTo;
	}

	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getContentTemplate() {
		return contentTemplate;
	}

	public void setContentTemplate(String contentTemplate) {
		this.contentTemplate = contentTemplate;
	}

	public Configuration getFreemarkerConf() {
		return freemarkerConf;
	}

	public void setFreemarkerConf(Configuration conf) {
		this.freemarkerConf = conf;
	}

	public String getContentHead() {
		return contentHead;
	}

	public void setContentHead(String contentHead) {
		this.contentHead = contentHead;
	}

	public String getContentFooter() {
		return contentFooter;
	}

	public void setContentFooter(String contentFooter) {
		this.contentFooter = contentFooter;
	}

	private void doSendEmail() throws Exception {
		MimeMessage message = javaMailSender.createMimeMessage();
		setMaiFields(message);
		javaMailSender.send(message);
	}

	private void setMaiFields(MimeMessage mimeMessage) throws IOException,TemplateException, MessagingException {
		MimeMessageHelper msg = new MimeMessageHelper(mimeMessage, true);
		msg.setSentDate(new Date());
		msg.setSubject(subject);
		msg.setFrom(from);
		msg.setTo(to);
		if(StringUtils.isNotBlank(replyTo)) {
			msg.setReplyTo(replyTo);
		}
		if(cc != null) {
			msg.setCc(cc);
		}
		if(bcc != null) {
			msg.setBcc(bcc);
		}
		
		msg.setText(processContentTemplate(),true);
	}

	private String processContentTemplate() throws IOException,TemplateException {
		Map model = new HashMap();
		List tempRows = rows;
		if(maxRows > 0) {
			rows.subList(0, Math.min(rows.size(), maxRows));
		}
		model.put("rows", tempRows);
		
		String text = contentTemplate;
		if(StringUtils.isNotBlank(contentHead)) {
			text = contentHead + text;
		}
		if(StringUtils.isNotBlank(contentFooter)) {
			text = text + contentFooter;
		}
		
		String mailContent = FreeMarkerTemplateUtils.processTemplateIntoString(new Template("",text,freemarkerConf), model);
		return mailContent;
	}

	@Override
	public void write(List<Object> rows) {
		this.rows.addAll(rows);
	}

	@Override
	public void close() throws IOException {
		try {
			doSendEmail();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
