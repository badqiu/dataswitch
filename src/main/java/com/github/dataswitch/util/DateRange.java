package com.github.dataswitch.util;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class DateRange {

	private Date start;
	private Date end;
	private String rangeType; //day,week,month
	/**
	 * String类型日期的格式，如yyyy-MM-dd 
	 **/
	private String dateFormat;
	
	public DateRange(Date start, Date end, String dateFormat,String rangeType) {
		this.start = start;
		this.end = end;
		this.rangeType = rangeType;
		this.dateFormat = dateFormat;
	}

	public String getStart() {
		return DateFormatUtils.format(start,dateFormat);
	}

	public String getEnd() {
		return DateFormatUtils.format(end,dateFormat);
	}
	
	public Date getStartDate() {
		return start;
	}

	public Date getEndDate() {
		return end;
	}
	
	public int getYear() {
		return getStartDate().getYear() + 1900;
	}
	
	public int getQuarter() {
		return getStartDate().getMonth() / 3 + 1;
	}

	public String getRangeType() {
		return rangeType;
	}

	@Override
	public String toString() {
		return "DateRange [start=" + getStart() + ", end=" + getEnd() + ", rangeType="
				+ rangeType + ", dateFormat=" + dateFormat + "]";
	}
	
	

}
