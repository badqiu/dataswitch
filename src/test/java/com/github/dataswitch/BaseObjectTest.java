package com.github.dataswitch;

import static org.junit.Assert.*;

import org.junit.Test;

public class BaseObjectTest {

	@Test
	public void setConfigByQuery() {
		BaseObject obj = new BaseObject();
		assertTrue(obj.isEnabled());
		assertEquals(obj.getId(),null);
		
		obj.setConfigByQuery("enabled=false&id=hello_id");
		
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
		
		obj.setConfigByQuery("  ");
		obj.setConfigByQuery(null);
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
	}
	
	@Test
	public void setConfigByProperties() {
		BaseObject obj = new BaseObject();
		assertTrue(obj.isEnabled());
		assertEquals(obj.getId(),null);
		
		obj.setConfigByProperties("enabled=false \n id=hello_id");
		
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
		
		obj.setConfigByProperties("  ");
		obj.setConfigByProperties(null);
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
	}
	
	@Test
	public void setConfigByJson() {
		BaseObject obj = new BaseObject();
		assertTrue(obj.isEnabled());
		assertEquals(obj.getId(),null);
		
		obj.setConfigByJson("{\"enabled\":false,\"id\":\"hello_id\"}");
		
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
		obj.setConfigByJson("  ");
		obj.setConfigByJson(null);
		assertFalse(obj.isEnabled());
		assertEquals(obj.getId(),"hello_id");
	}


}
