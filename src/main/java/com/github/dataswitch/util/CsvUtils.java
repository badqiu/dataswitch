package com.github.dataswitch.util;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class CsvUtils {
    
    // CSV 中使用的引号字符
    private static final char QUOTE_CHAR = '"';
    // CSV 中的转义引号 (双引号)
    private static final String ESCAPED_QUOTE = "\"\"";
    // 日期时间格式
    private static final DateTimeFormatter DATE_TIME_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // 空值表示
    private static final String NULL_VALUE = "";
    
    public String toCsvStringValue(Object v) {
        if (v == null) {
            return NULL_VALUE;
        }
        
        // 处理日期类型
        if (v instanceof Date) {
        	LocalDateTime ldt = ((Date) v).toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();
            return DATE_TIME_FORMATTER.format(ldt);
        }
        
        // 处理 Java 8+ 时间类型
        if (v instanceof Temporal) {
            return DATE_TIME_FORMATTER.format((Temporal) v);
        }
        
        // 处理字符串类型 - 需要添加引号和转义
        if (v instanceof String) {
            return escapeAndQuoteString((String) v);
        }
        
        // 处理集合类型
        if (v instanceof Collection) {
            return handleCollection((Collection<?>) v);
        }
        
        // 处理数组类型
        if (v.getClass().isArray()) {
            return handleArray(v);
        }
        
        // 处理 Map 类型
        if (v instanceof Map) {
            return handleMap((Map<?, ?>) v);
        }
        
        // 其他类型使用默认的 toString()
        return Objects.toString(v);
    }
    
    private String escapeAndQuoteString(String value) {
        // 如果字符串包含引号、逗号或换行符，需要用引号括起来并转义引号
        if (value.indexOf(QUOTE_CHAR) >= 0 || 
            value.indexOf(',') >= 0 || 
            value.contains("\n") || 
            value.contains("\r")) {
            
            // 转义双引号
            value = value.replace(String.valueOf(QUOTE_CHAR), ESCAPED_QUOTE);
            // 用引号括起来
            return QUOTE_CHAR + value + QUOTE_CHAR;
        }
        
        return value;
    }
    
    private String handleCollection(Collection<?> collection) {
        StringBuilder sb = new StringBuilder();
        sb.append(QUOTE_CHAR);
        
        boolean first = true;
        for (Object item : collection) {
            if (!first) {
                sb.append(",");
            }
            sb.append(toCsvStringValue(item));
            first = false;
        }
        
        sb.append(QUOTE_CHAR);
        return sb.toString();
    }
    
    private String handleArray(Object array) {
        // 转换为对象数组
        Object[] objectArray = toObjectArray(array);
        return handleCollection(Arrays.asList(objectArray));
    }
    
    private Object[] toObjectArray(Object array) {
        if (array instanceof Object[]) {
            return (Object[]) array;
        }
        
        // 处理基本类型数组
        int length = java.lang.reflect.Array.getLength(array);
        Object[] result = new Object[length];
        for (int i = 0; i < length; i++) {
            result[i] = java.lang.reflect.Array.get(array, i);
        }
        return result;
    }
    
    private String handleMap(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder();
        sb.append(QUOTE_CHAR);
        
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append(toCsvStringValue(entry.getKey()));
            sb.append(":");
            sb.append(toCsvStringValue(entry.getValue()));
            first = false;
        }
        
        sb.append(QUOTE_CHAR);
        return sb.toString();
    }
}