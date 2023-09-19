package com.github.dataswitch.input;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.processor.Processor;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.MapUtil;

public class BizFileInputTest {
	private Map[] fileProps = new Map[]{
			MapUtil.newMap("include","游戏下载分析*.xls","columns","日期,游戏中心下载量,应用商店下载量,当日总下载量,游戏中心新增激活,新增激活用户,总点击数,页面转化率,激活转化率"),
			MapUtil.newMap("include","游戏活跃留存分析*.xls","columns","日期,当日总下载量,活跃,七日活跃,三十日活跃,次日留存,三日留存,七日留存,十日留存,三十日留存"),
			MapUtil.newMap("include","游戏付费分析*.xls","columns","日期,当日总下载量,当日付费金额_mobile,当日付费金额_pad,总付费额,总付费用户数,付费率,新增付费金额,新增付费用户数,新增付费率,付费Arpu,下载Arpu,日活Arpu")
	};
	
	@Test
	public void test() throws Exception {
		
		JdbcOutput jdbcOutput = new JdbcOutput();
		jdbcOutput.setUsername("root");
		jdbcOutput.setPassword("");
		jdbcOutput.setUrl("jdbc:mysql://localhost:3306/st_other?useUnicode=true&characterEncoding=UTF-8");
		jdbcOutput.setDriverClass("com.mysql.jdbc.Driver");
		jdbcOutput.setSql("insert st_app_kv(tdate,tdate_type,app_id,kpi_key,kpi_value) values(:日期,'day',:gameName,:key,:value) ON DUPLICATE KEY UPDATE kpi_value=values(kpi_value)");
		for(File dir : new File("E:/tmp/金山云").listFiles()) {
			readFile2DataBase(dir,jdbcOutput);
		}
		for(File dir : new File("E:/tmp/金山云").listFiles()) {
			System.out.println(dir.getName());
		}
	}

	private void readFile2DataBase(File dir,JdbcOutput jdbcOutput) throws Exception {
		
		String gameName = dir.getName();
		for(Map<String,String> props : fileProps) {
			TxtFileInput input = new TxtFileInput();
			String path = dir.getAbsolutePath();
			input.setDir(path);
			input.setInclude(props.get("include"));
			input.setColumns(props.get("columns"));
			input.setSkipLines(1);
			input.setColumnSeparator("\t");
			input.setCharset("GBK");
			List<Map> rows = MapHelper.one2ManyRows(input.read(10000),"日期");
			MapHelper.allPutAll(rows,MapUtil.newMap("gameName",gameName));
			
			Object[] finalRows = rows.stream().filter(new Predicate<Map>() {
				public boolean test(Map t) {
					if(t.get("日期").equals("总计")) {
						return false;
					}
					return true;
				};
			}).toArray();
			for(Object row : finalRows) {
//				System.out.println(row);
			}
			
			Processor processor = percentValueProcessor();
			
			InputOutputUtil.write(jdbcOutput, Arrays.asList(finalRows), processor);
		}
	}

	private Processor percentValueProcessor() {
		Processor processor = new Processor() {
			public List<Object> process(List datas) throws Exception {
				List<Object> result = new ArrayList();
				for(Map row : (List<Map>)datas) {
					String value = (String)row.get("value");
					row.put("value", ""+percentString2Number(value));
					result.add(row);
				}
				return result;
			}
		};
		return processor;
	}

	public static double percentString2Number(String input) {
		if(StringUtils.isBlank(input)) {
			return 0;
		}
		double result = 0;
		int indexOf = input.indexOf("%");
		if(indexOf>0) {
			String tempNum = input.substring(0,indexOf);
			result = Double.parseDouble(tempNum) / 100;
		}else {
			result = Double.parseDouble(input);
		}
		return result;
	}
	
	public static class MapHelper {
		public static void allPutAll(List<Map> rows, Map commonMap) {
			for(Map row : rows) {
				row.putAll(commonMap);
			}
		}
	
		private static List<Map> one2ManyRows(List<Object> inputs,String... removeKeys) {
			List<Map> outputs = new ArrayList<Map>();
			for(Object obj : inputs) {
				Map common = (Map)obj;
				List<Map<String, Object>> rows = explode(mapRemove(common,removeKeys));
				for(Map row : rows ){
					Map output = new HashMap(common);
					output.putAll(row);
					outputs.add(output);
				}
			}
			return outputs;
		}
		
		public static Map<String,Object> mapRemove(Map<String, Object> input,String... keys) {
			if(input == null) return null;
			if(keys == null) return input;
			
			Map output = new HashMap(input);
			for(String key : keys) {
				output.remove(key);
			}
			return output;
		}
		
		public static List<Map<String,Object>> explode(Map<String, Object> map) {
			List<Map<String,Object>> rows = new ArrayList();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				Map row = MapUtil.newMap("key",key,"value",value);
				rows.add(row);
			}
			return rows;
		}
	}

	
}
