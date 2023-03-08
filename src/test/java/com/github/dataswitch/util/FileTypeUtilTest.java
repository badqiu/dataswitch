package com.github.dataswitch.util;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class FileTypeUtilTest {

	@Test
	public void test_java() {
		List<String> comments = FileTypeUtil.getAllComments("hello.java", "import aaa;\n //hello\n /* !generate(hello,123) */");
		System.out.println(comments);
		assertEquals("[ !generate(hello,123) , hello]",comments.toString());
	}

	@Test
	public void test_xml() {
		List<String> comments = FileTypeUtil.getAllComments("hello.xml", "<!-- hello --><bean><sql>select * from a</sql></bean><!-- end -->");
		System.out.println(comments);
		assertEquals("[ hello ,  end ]",comments.toString());
	}
	
	@Test
	public void test_properties() {
		List<String> comments = FileTypeUtil.getAllComments("hello.properties", "# hello\nkey=value\n#diy");
		System.out.println(comments);
		assertEquals("[ hello, diy]",comments.toString());
	}
	
	@Test
	public void test_sql() {
		List<String> comments = FileTypeUtil.getAllComments("hello.sql", "import aaa;\n --hello\n /* !generate(hello,123) */");
		System.out.println(comments);
		assertEquals("[ !generate(hello,123) , hello]",comments.toString());
	}
	
	@Test
	public void test_python() {
		List<String> comments = FileTypeUtil.getAllComments("hello.py", "import aaa;\n #hello\n /* !generate(hello,123) */");
		System.out.println(comments);
		assertEquals("[hello]",comments.toString());
	}
}
