package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.springframework.core.serializer.Deserializer;
import org.springframework.util.Assert;

import com.github.dataswitch.util.Util;

public class ExcelDeserializer  implements Deserializer{

	/**
	 * 数据列
	 */
	private String columns;
	/**
	 * 忽略跳过的列数
	 */
	private int skipLines = 0;
	
	private transient String[] columnNames;
	
	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
		Assert.hasText(columns,"columns must be not empty");
		columnNames = Util.splitColumns(columns);
	}

	public int getSkipLines() {
		return skipLines;
	}

	public void setSkipLines(int skipLines) {
		this.skipLines = skipLines;
	}

	@Override
	public Object deserialize(InputStream inputStream) throws IOException {
		POIFSFileSystem fs = new POIFSFileSystem(inputStream);
	    HSSFWorkbook wb = new HSSFWorkbook(fs);
	    FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
	    HSSFSheet sheet = wb.getSheetAt(0);

	    int totalRows = sheet.getPhysicalNumberOfRows();
	    int cols = getSheetColumns(sheet, totalRows);
	    List<Map> result = new ArrayList<Map>(totalRows);
	    for(int i = skipLines; i < totalRows; i++) {
	    	HSSFRow row = sheet.getRow(i);
	        if(row != null) {
	        	Map rowMap = new LinkedHashMap();
	            for(int c = 0; c < cols; c++) {
	            	HSSFCell cell = row.getCell(c);
	                if(cell != null && i < columnNames.length) {
	                    String columnName = columnNames[c];
	                    rowMap.put(columnName, getCellValue(evaluator,cell));
	                }
	            }
	            result.add(rowMap);
	        }
	    }
	    
		return result;
	}

	private static Object getCellValue(FormulaEvaluator evaluator, HSSFCell cell) {
//		switch (evaluator.evaluateFormulaCell(cell)) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BOOLEAN:
			return cell.getBooleanCellValue();
		case Cell.CELL_TYPE_NUMERIC:
			return cell.getNumericCellValue();
		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();
		case Cell.CELL_TYPE_BLANK:
			break;
		case Cell.CELL_TYPE_ERROR:
			return cell.getErrorCellValue();
		case Cell.CELL_TYPE_FORMULA:
			break;
		}
		return null;
	}

	private int getSheetColumns(HSSFSheet sheet, int rows) {
		HSSFRow row;
		int tmp = 0;
	    int cols = 0; // No of columns
	    // This trick ensures that we get the data properly even if it doesn't start from first few rows
	    for(int i = 0;  i < rows; i++) {
	        row = sheet.getRow(i);
	        if(row != null) {
	            tmp = sheet.getRow(i).getPhysicalNumberOfCells();
	            if(tmp > cols) 
	            	cols = tmp;
	        }
	    }
		return cols;
	}

}
