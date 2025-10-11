package com.github.dataswitch.util;

import static com.github.dataswitch.util.StringRemoveUtil.removeControlCharactersLoop;
import static com.github.dataswitch.util.StringRemoveUtil.removeControlCharactersStringBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class StringRemoveUtilTest {

	 // --- 测试原正则表达式方法（作为基准） ---
    private String removeControlCharactersOriginal(String csvStringValue) {
        if (csvStringValue == null) return null;
        return csvStringValue.replaceAll("[\u0000-\u001F]", "");
    }

    // --- 测试数据 ---
    @Test
    public void testNormalString() {
        String input = "Hello,\tWorld!\nThis is a test.";
        String expected = "Hello,World!This is a test."; // 移除了\t和\n
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

    @Test
    public void testStringWithVariousControlChars() {
        // 包含换行(\n)、回车(\r)、制表符(\t)等
        String input = "Line1\nLine2\rTab\tHere" + (char)0x1F + "End";
        String expected = "Line1Line2TabHereEnd"; // 所有控制字符应被移除
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

    @Test
    public void testStringWithNoControlChars() {
        String input = "This string has no control characters.";
        String expected = input; // 应保持不变
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

    @Test
    public void testStringWithOnlyControlChars() {
        String input = "\n\r\t" + (char)0x00 + (char)0x1F;
        String expected = ""; // 应得到空字符串
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

    @Test
    public void testEmptyString() {
        String input = "";
        String expected = "";
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

    @Test
    public void testNullInput() {
        assertNull("Original method", removeControlCharactersOriginal(null));
        assertNull("StringBuilder method", removeControlCharactersStringBuilder(null));
        assertNull("Loop method", removeControlCharactersLoop(null));
    }

    @Test
    public void testUnicodeAboveControlRange() {
        // 包含控制字符范围之后的字符，如空格(' ', 0x20)和普通字符
        String input = "Before" + (char)0x1F + "After" + (char)0x20 + "End";
        String expected = "BeforeAfter End"; // 0x1F被移除，0x20(空格)保留
        
        assertEquals("Original method", expected, removeControlCharactersOriginal(input));
        assertEquals("StringBuilder method", expected, removeControlCharactersStringBuilder(input));
        assertEquals("Loop method", expected, removeControlCharactersLoop(input));
    }

}
