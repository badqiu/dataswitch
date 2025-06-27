package com.github.dataswitch.util;

import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

public class DateUtilTest {
	
	@Test
    public void generateDateTimeLoop() throws ParseException {
        Date start = DateUtils.parseDate("2025-06-01 12:00", "yyyy-MM-dd HH:mm");
        Date end = DateUtils.parseDate("2025-06-01 18:00", "yyyy-MM-dd HH:mm");
        List<Date> timeList = DateUtil.generateDateTimeLoop(start, end, 30);

        // 打印结果：按 yyyy-MM-dd HH:mm 格式化
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        timeList.forEach(dt -> System.out.println(DateFormatUtils.format(dt, "yyyy-MM-dd HH:mm:ss")));
    }
}
