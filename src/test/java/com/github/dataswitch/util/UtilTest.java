package com.github.dataswitch.util;

import static org.junit.Assert.*;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class UtilTest {

	@Test
	public void test() {
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user,age"),";"));
		assertEquals("user;age",StringUtils.join(Util.splitColumns("user  age"),";"));
		assertEquals("user;age;diy;blog;abc",StringUtils.join(Util.splitColumns("user\nage\n\t\ndiy\nblog,abc"),";"));
		
		assertEquals("tdate;tdate_type;app_id;channel_id;pay;pay_account_idcnt;pay_device_idcnt;active_device_idcnt;active_account_idcnt",StringUtils.join(Util.splitColumns("				tdate,tdate_type,app_id,channel_id,\n				pay,pay_account_idcnt,pay_device_idcnt,active_device_idcnt,active_account_idcnt\n\t\n\t \n"),";"));
		
	}

}
