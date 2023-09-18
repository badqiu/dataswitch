package com.github.dataswitch.runner;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import com.github.dataswitch.util.DateRange;
import com.github.dataswitch.util.DateUtil;

public class DefaultParam {

	public static Map withDefaultParams(Map params,String dayKey,String dateFormat) {
		Map result = new HashMap(params);
		result.put("env", System.getenv());
		
		String day = (String)params.get(dayKey);
		addDayWeekMonthDateRangeList(result, day,dateFormat);
		
		addUtilsClass(result);
		return result;
	}


	private static void addUtilsClass(Map result) {
		result.put("DateUtil", new DateUtil());
		result.put("DateUtils", new DateUtils());
		result.put("StringUtils", new StringUtils());
	}

	private static void addDayWeekMonthDateRangeList(Map result, String day,String dateFormat) {
		if(StringUtils.isBlank(day)) {
			return;
		}
		
		Date date = DateUtil.parse(day, dateFormat);
		List<DateRange> dayWeekMonth = DateUtil.genDayWeekMonth(date, dateFormat);
		DateRange week = new DateRange(DateUtil.getStartDate(date,"week"),DateUtil.getEndDate(date,"week"),dateFormat,"week");
		DateRange month = new DateRange(DateUtil.getStartDate(date,"month"),DateUtil.getEndDate(date,"month"),dateFormat,"month");
		DateRange quarter = new DateRange(DateUtil.getStartDate(date,"quarter"),DateUtil.getEndDate(date,"quarter"),dateFormat,"quarter");
		DateRange year = new DateRange(DateUtil.getStartDate(date,"year"),DateUtil.getEndDate(date,"year"),dateFormat,"year");
		
		result.put("hour",DateUtil.format(date, "yyyyMMddHH"));
		result.put("day_week_month",dayWeekMonth);
		result.put("week",week);
		result.put("month",month);
		result.put("quarter",quarter);
		result.put("year",year);
	}
	
}
