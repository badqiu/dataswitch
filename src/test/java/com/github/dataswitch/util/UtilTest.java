package com.github.dataswitch.util;

import static org.junit.Assert.*;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class UtilTest {

	@Test
	public void test() {
		assertEquals("",StringUtils.join(Util.splitColumns(" \t\n ,,,"),";"));
		assertEquals(null,StringUtils.join(Util.splitColumns(null),";"));
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user,age"),";"));
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user  age"),";"));
		assertEquals("user;age;diy;blog;abc",StringUtils.join(Util.splitColumns("user\nage\n\t\ndiy\nblog,abc"),";"));
		assertEquals("user;age;diy;blog;abc",StringUtils.join(Util.splitColumns("user\nage\n\t\n\t\t diy\nblog,abc"),";"));
		assertEquals("user;age;diy;blog;abc",StringUtils.join(Util.splitColumns("user ,\nage,\n\t\ndiy,\nblog,abc"),";"));
		
		assertEquals("tdate;tdate_type;app_id;channel_id;pay;pay_account_idcnt;pay_device_idcnt;active_device_idcnt;active_account_idcnt",StringUtils.join(Util.splitColumns("				tdate,tdate_type,app_id,channel_id,\n				pay,pay_account_idcnt,pay_device_idcnt,active_device_idcnt,active_account_idcnt\n\t\n\t \n"),";"));
		
	}
	
	@Test
	public void underscoreName() {
		assertEquals(null,Util.underscoreName(null));
		assertEquals(" ",Util.underscoreName(" "));
		assertEquals("  ",Util.underscoreName("  "));
		assertEquals("user_name",Util.underscoreName("userName"));
		assertEquals("user_name_sex1",Util.underscoreName("userNameSex1"));
		assertEquals("user_name_sex1",Util.underscoreName("user_name_sex1"));
	}

}
