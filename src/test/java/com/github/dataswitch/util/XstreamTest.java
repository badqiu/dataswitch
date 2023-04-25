package com.github.dataswitch.util;

import java.io.Writer;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.Test;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.core.util.QuickWriter;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XppDriver;
import com.thoughtworks.xstream.security.NullPermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;

public class XstreamTest {

	public static XStream getInstance() {
		XppDriver hierarchicalStreamDriver = new XppDriver() {
			@Override
			public HierarchicalStreamWriter createWriter(Writer out) {
				PrettyPrintWriter prettyPrintWriter = new PrettyPrintWriter(out, getNameCoder()) {
					protected String PREFIX_CDATA = "<![CDATA[";
					protected String SUFFIX_CDATA = "]]>";
					protected int CDATA_LENGTH = 50;

					@Override
					protected void writeText(QuickWriter writer, String text) {
						if (text.startsWith(PREFIX_CDATA) && text.endsWith(SUFFIX_CDATA)) {
							writer.write(text);
						} else {
							if(text.length() >= CDATA_LENGTH || text.contains("\n")) {
								writer.write(PREFIX_CDATA);
								writer.write(text);
								writer.write(SUFFIX_CDATA);
							}else {
								super.writeText(writer, text);
							}
						}
					}
				};

				return prettyPrintWriter;
			}
		};

		XStream xstream = new XStream(hierarchicalStreamDriver);

		xstream.ignoreUnknownElements();
		xstream.setMode(XStream.NO_REFERENCES);
		xstream.addPermission(NullPermission.NULL);
		xstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
		return xstream;
	}

	@Test
	public void test() {
		XStream x = getInstance();
		TestBean b = new TestBean();
		b.name = "<![CDATA[test_name--------------------;&nbsp;--------------------------\n--]]>";
		b.longDesc = "long,k,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\n,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,long\n";
		String xml = x.toXML(b);
		System.out.println(xml);
		
		TestBean fromXmlBean = (TestBean)x.fromXML(xml);
		System.out.println(ToStringBuilder.reflectionToString(fromXmlBean));
	}

	public static class TestBean {
		private String name;
		private long id;
		private String longDesc;
//		public String getName() {
//			return name;
//		}
//		public void setName(String name) {
//			this.name = name;
//		}
//		public long getId() {
//			return id;
//		}
//		public void setId(long id) {
//			this.id = id;
//		}
	}
}
