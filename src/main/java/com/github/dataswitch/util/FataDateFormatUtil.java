package com.github.dataswitch.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FataDateFormatUtil {
	private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    // 日期时间格式
    public static final DateTimeFormatter DATE_TIME_FORMATTER = 
        DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
    
	public static String formatDateTime(Date date) {
	    if (date == null) return null;

	    try {
	        if (date instanceof java.sql.Time) {
	            // 专门处理 Time：转换为 LocalTime
	            java.sql.Time sqlTime = (java.sql.Time) date;
	            // 根据需要返回时间字符串，例如 "10:30:45"
	            return sqlTime.toLocalTime().toString();
	        } else if (date instanceof java.sql.Date) {
	            // 专门处理 sql.Date：转换为当天的开始时刻
	            java.sql.Date sqlDate = (java.sql.Date) date;
	            Instant instant = sqlDate.toLocalDate().atStartOfDay(ZoneId.systemDefault()).toInstant();
	            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
	            return DATE_TIME_FORMATTER.format(localDateTime);
	        } else {
	            // 处理 java.util.Date 和 java.sql.Timestamp
	            Instant instant = date.toInstant();
	            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
	            return DATE_TIME_FORMATTER.format(localDateTime);
	        }
	    } catch (Exception e) {
	        // 日志记录异常 e，并返回一个安全值，例如空字符串或特定提示
	        return ""; 
	    }
	}
	
}
