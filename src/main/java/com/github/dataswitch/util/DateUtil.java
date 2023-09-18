package com.github.dataswitch.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;


public class DateUtil {

	static String DATE_FORMAT = "yyyy-MM-dd";
	
	public static List<DateRange> genDayWeekMonth(Date date,String dateFormat) {
		List<DateRange> result = new ArrayList<DateRange>();
		DateRange day = new DateRange(date,date,dateFormat,"day");
		DateRange week = new DateRange(getStartDate(date,"week"),getEndDate(date,"week"),dateFormat,"week");
		DateRange month = new DateRange(getStartDate(date,"month"),getEndDate(date,"month"),dateFormat,"month");
		
		if(!day.getEnd().equals(week.getEnd())) {
			week = null;
		}
		if(!day.getEnd().equals(month.getEnd())) {
			month = null;
		}
		
		CollectionUtils.addIgnoreNull(result, day);
		CollectionUtils.addIgnoreNull(result, week);
		CollectionUtils.addIgnoreNull(result, month);
		
		return result;
	}
	
	// 返回日历
	public static Calendar getCalendar(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar;
	}
	
	public static Date getStartDate(Date date,String rangeType) {
		Calendar calendar = getCalendar(date);
		if (rangeType.equals("week")) {
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		}
		if (rangeType.equals("month")) {
			calendar.set(Calendar.DAY_OF_MONTH, 1);
		}
		if (rangeType.equals("quarter")) {
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			int month = calendar.get(Calendar.MONTH);
			int quarter = getQuarterByMonth(month); 
			calendar.set(Calendar.MONTH, quarter * 3 );
		}
		if (rangeType.equals("year")) {
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			calendar.set(Calendar.MONTH, 0 );
		}
		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}

	private static int getQuarterByMonth(int month) {
		int quarter = ((month)/ 3);
		return quarter;
	}
	
	public static Date getEndDate(Date date,String rangeType)
	{
		if (date == null || rangeType == null)
			throw new RuntimeException("Date and rangeType must be not null");
		Calendar calendar = getCalendar(date);
		if (rangeType.equals("week")) {
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		}
		if (rangeType.equals("month")) {
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			calendar.roll(Calendar.DAY_OF_MONTH, -1);
		}
		if (rangeType.equals("quarter")) {
			int month = calendar.get(Calendar.MONTH);
			int quarter = getQuarterByMonth(month); 
			calendar.set(Calendar.MONTH, quarter * 3 + 2);
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			calendar.roll(Calendar.DAY_OF_MONTH, -1);
		}
		if (rangeType.equals("year")) {
			calendar.set(Calendar.MONTH, 11);
			calendar.set(Calendar.DAY_OF_MONTH, 31);
		}

		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		calendar.set(Calendar.MILLISECOND, 999);
		return calendar.getTime();
	}

	// 2天之间相差多少天
	public static int dayDiff(Date start, Date end) {
		Calendar startCalendar = DateUtil.getCalendar(start);
		Calendar endCalendar = DateUtil.getCalendar(end);
		startCalendar.set(Calendar.HOUR_OF_DAY, 0);
		startCalendar.set(Calendar.MINUTE, 0);
		startCalendar.set(Calendar.SECOND, 0);
		startCalendar.set(Calendar.MILLISECOND, 0);
		endCalendar.set(Calendar.HOUR_OF_DAY, 0);
		endCalendar.set(Calendar.MINUTE, 0);
		endCalendar.set(Calendar.SECOND, 0);
		endCalendar.set(Calendar.MILLISECOND, 0);
		long millSecDiff = endCalendar.getTimeInMillis() - startCalendar.getTimeInMillis();
		int dayDiff = (int) Math.round((millSecDiff / (1000 * 60 * 60 * 24)));
		return dayDiff;
	}
	
	
	public static String addDays(String dateString,int amount) {
		return addDays(dateString,DATE_FORMAT,amount);
	}
	
	public static String addWeeks(String dateString,int amount) {
		return addWeeks(dateString,DATE_FORMAT,amount);
	}
	
	public static String addMonths(String dateString,int amount) {
		return addMonths(dateString,DATE_FORMAT,amount);
	}
	
	public static String format(String dateString,String newDateFormat) {
		if(StringUtils.isBlank(dateString)) return null;
		Date date = parse(dateString, DATE_FORMAT);
		return format(date, newDateFormat);
	}
	
	public static String addDays(String dateString,String dateFormat,int amount) {
		if(StringUtils.isBlank(dateString)) {
			return null;
		}
		Date date = parse(dateString, dateFormat);
		Date result = DateUtils.addDays(date, amount);
		return format(result, dateFormat);
	}
		
	public static String addWeeks(String dateString,String dateFormat,int amount) {
		if(StringUtils.isBlank(dateString)) {
			return null;
		}
		Date date = parse(dateString, dateFormat);
		Date result = DateUtils.addWeeks(date, amount);
		return format(result, dateFormat);
	}
	


	public static String addMonths(String dateString,String dateFormat,int amount) {
		if(StringUtils.isBlank(dateString)) {
			return null;
		}
		Date date = parse(dateString, dateFormat);
		Date result = DateUtils.addMonths(date, amount);
		return format(result, dateFormat);
	}

	public static Date parse(String dateString, String dateFormat) {
		if(dateString == null) return null;
		
		try {
			return new SimpleDateFormat(dateFormat).parse(dateString);
		} catch (ParseException e) {
			throw new RuntimeException("cannot parse dateString:"+dateString+" with format:"+dateFormat);
		}
	}
	
	public static String format(Date date, String dateFormat) {
		if(date == null) return null;
		
		return new SimpleDateFormat(dateFormat).format(date);
	}
	
}
