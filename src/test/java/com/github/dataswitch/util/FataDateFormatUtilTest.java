package com.github.dataswitch.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class FataDateFormatUtilTest {

    @Test
    public void testFormatDateTimeWithNull() {
        // 测试空值情况
        String result = FataDateFormatUtil.formatDateTime(null);
        assertNull("传入null应该返回null", result);
    }

    @Test
    public void testFormatDateTimeWithUtilDate() throws ParseException {
        // 测试 java.util.Date
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date utilDate = sdf.parse("2023-10-13 15:30:45");
        
        String result = FataDateFormatUtil.formatDateTime(utilDate);
        assertEquals("java.util.Date 应该格式化为 yyyy-MM-dd HH:mm:ss", 
                     "2023-10-13 15:30:45", result);
    }

    @Test
    public void testFormatDateTimeWithSqlDate() {
        // 测试 java.sql.Date - 注意：sql.Date 只包含日期部分，时间部分会被忽略
        java.sql.Date sqlDate = java.sql.Date.valueOf("2023-10-13");
        
        String result = FataDateFormatUtil.formatDateTime(sqlDate);
        // sql.Date 应该被格式化为当天的零点时刻
        assertEquals("java.sql.Date 应该格式化为当天的 00:00:00", 
                     "2023-10-13 00:00:00", result);
    }

    @Test
    public void testFormatDateTimeWithSqlTime() {
        // 测试 java.sql.Time
        java.sql.Time sqlTime = java.sql.Time.valueOf("15:30:45");
        
        String result = FataDateFormatUtil.formatDateTime(sqlTime);
        assertEquals("java.sql.Time 应该格式化为 HH:mm:ss", 
                     "15:30:45", result);
    }

    @Test
    public void testFormatDateTimeWithSqlTimestamp() {
        // 测试 java.sql.Timestamp (继承自 java.util.Date)
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2023-10-13 15:30:45.123");
        
        String result = FataDateFormatUtil.formatDateTime(timestamp);
        // Timestamp 会被正确处理，但注意毫秒部分会被忽略（因为格式中不包含毫秒）
        assertEquals("java.sql.Timestamp 应该正确格式化", 
                     "2023-10-13 15:30:45", result);
    }

    @Test
    public void testFormatDateTimeWithDifferentDates() throws ParseException {
        // 测试边界日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        // 测试最小日期
        Date minDate = sdf.parse("1970-01-01 00:00:00");
        assertEquals("1970-01-01 00:00:00", FataDateFormatUtil.formatDateTime(minDate));
        
        // 测试未来日期
        Date futureDate = sdf.parse("2030-12-31 23:59:59");
        assertEquals("2030-12-31 23:59:59", FataDateFormatUtil.formatDateTime(futureDate));
    }

    @Test
    public void testFormatDateTimeConsistency() throws ParseException {
        // 测试相同时间点的不同Date类型是否产生一致的结果
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date utilDate = sdf.parse("2023-10-13 15:30:45");
        
        // 创建相同时间的sql.Date（注意：sql.Date会忽略时间部分）
        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        
        String utilResult = FataDateFormatUtil.formatDateTime(utilDate);
        String sqlResult = FataDateFormatUtil.formatDateTime(sqlDate);
        
        // utilDate 应该保持完整时间，sqlDate 应该转为零点
        assertEquals("java.util.Date 应该保持原时间", "2023-10-13 15:30:45", utilResult);
        assertEquals("java.sql.Date 应该转为零点", "2023-10-13 00:00:00", sqlResult);
    }

    @Test
    public void testFormatDateTimeExceptionHandling() {
        // 测试异常处理 - 创建一个可能引发异常的场景
        // 这里我们依赖方法内部的try-catch来确保不会抛出异常
        
        // 创建一个正常的日期，确保方法能够处理而不抛出异常
        Date normalDate = new Date();
        String result = FataDateFormatUtil.formatDateTime(normalDate);
        
        // 只要不抛出异常，并且返回非空字符串，就认为测试通过
        assertNotNull("正常日期应该返回非空结果", result);
        assertFalse("正常日期应该返回非空字符串", result.isEmpty());
    }
}