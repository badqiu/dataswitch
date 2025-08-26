package com.github.dataswitch.eshop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.google.gson.Gson;

/**
 * 抖店-商机中心-爬虫数据批量插入实现
 * 
 */
public class ClueDataDao {
    private static final String DB_URL = "jdbc:mysql://107.173.159.122:3306/eshop_test?useInformationSchema=true&nullDatabaseMeansCurrent=true&useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&rewriteBatchedStatements=true";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "pwd_dkSjdjkJfffVVjd23hf82kdsfusdfhjhdhfjijfjfuuHFhdfdjysywO";
    
    // 批量插入的SQL语句，与单条插入相同
//    private static final String BATCH_INSERT_SQL = "INSERT INTO clue_data (clue_detail, submit_times, is_collect, is_grant, "
//                   + "query_clue_card_info, product_diagnose_result, auto_submit_task, "
//                   + "cate_qualification_open, has_cate_qualification, word_clue_indicator_info, "
//                   + "category_clue_extra_info, hot_sale_products, clue_indicator, clue_collect_info,batch_date,keyword) "
//                   + "VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    // 批量插入的SQL语句，使用命名参数
    private static final String BATCH_INSERT_OR_UPDATE_SQL = "INSERT INTO clue_data (clue_detail, submit_times, is_collect, is_grant, "
            + "query_clue_card_info, product_diagnose_result, auto_submit_task, "
            + "cate_qualification_open, has_cate_qualification, word_clue_indicator_info, "
            + "category_clue_extra_info, hot_sale_products, clue_indicator, clue_collect_info, batch_date, keyword) "
            + "VALUES (:clueDetailJson, :submitTimes, :isCollect, :isGrant, "
            + ":queryClueCardInfoJson, :productDiagnoseResultJson, :autoSubmitTaskJson, "
            + ":cateQualificationOpen, :hasCateQualification, :wordClueIndicatorInfoJson, "
            + ":categoryClueExtraInfoJson, :hotSaleProductsJson, :clueIndicatorJson, :clueCollectInfoJson, :batchDate, :keyword) "
            
            + "ON DUPLICATE KEY UPDATE "
            + "clue_detail = VALUES(clue_detail), "
            + "submit_times = VALUES(submit_times), "
            + "is_collect = VALUES(is_collect), "
            + "is_grant = VALUES(is_grant), "
            + "query_clue_card_info = VALUES(query_clue_card_info), "
            + "product_diagnose_result = VALUES(product_diagnose_result), "
            + "auto_submit_task = VALUES(auto_submit_task), "
            + "cate_qualification_open = VALUES(cate_qualification_open), "
            + "has_cate_qualification = VALUES(has_cate_qualification), "
            + "word_clue_indicator_info = VALUES(word_clue_indicator_info), "
            + "category_clue_extra_info = VALUES(category_clue_extra_info), "
            + "hot_sale_products = VALUES(hot_sale_products), "
            + "clue_indicator = VALUES(clue_indicator), "
            + "clue_collect_info = VALUES(clue_collect_info), "
            + "batch_date = VALUES(batch_date), "
            + "keyword = VALUES(keyword)";
   
    
    // Gson实例作为静态变量，避免重复创建
    private static final Gson gson = new Gson();

    int batchSize = 500;
    
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private JdbcTemplate jdbcTemplate;
    
    public ClueDataDao() {
        DataSource dataSource = createDataSource();
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    private DataSource createDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(DB_USER);
        dataSource.setPassword(DB_PASSWORD);
        return dataSource;
    }
    
    /**
     * 批量保存ClueData列表
     * @param clueDataList 要保存的ClueData对象列表
     */
    public void batchSaveClueData(List<ClueData> clueDataList) {
    	batchSaveClueDataWithNamedParams(clueDataList);
//        batchSaveWithPrepareStatement(clueDataList);
    }

	private void batchSaveWithPrepareStatement(List<ClueData> clueDataList) {
		if (clueDataList == null || clueDataList.isEmpty()) {
            return; // 如果列表为空，直接返回
        }
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(BATCH_INSERT_OR_UPDATE_SQL)) {
            
            // 关闭自动提交，手动控制事务
            conn.setAutoCommit(false);
            
            int count = 0;
            for (ClueData clueData : clueDataList) {
                // 设置参数
            	if(count == 1) {
            		System.out.println("count=1 data:"+gson.toJson(clueData));
            	}
            	
                pstmt.setString(1, gson.toJson(clueData.clue_detail));
                pstmt.setInt(2, clueData.submit_times);
                pstmt.setBoolean(3, clueData.is_collect);
                pstmt.setBoolean(4, clueData.is_grant);
                pstmt.setString(5, gson.toJson(clueData.query_clue_card_info));
                pstmt.setString(6, gson.toJson(clueData.product_diagnose_result));
                pstmt.setString(7, gson.toJson(clueData.auto_submit_task));
                pstmt.setBoolean(8, clueData.cate_qualification_open);
                pstmt.setBoolean(9, clueData.has_cate_qualification);
                pstmt.setString(10, gson.toJson(clueData.word_clue_indicator_info));
                pstmt.setString(11, gson.toJson(clueData.category_clue_extra_info));
                pstmt.setString(12, gson.toJson(clueData.hot_sale_products));
                pstmt.setString(13, gson.toJson(clueData.clue_indicator));
                pstmt.setString(14, gson.toJson(clueData.clue_collect_info));
                pstmt.setTimestamp(15, new Timestamp(clueData.batch_date.getTime()));
                pstmt.setString(16, clueData.getKeyword());
                
                
                // 添加到批处理
                pstmt.addBatch();
                
				if (++count % batchSize == 0) {
                    pstmt.executeBatch();
                    System.out.println("batchSaveClueData() batchSize="+batchSize+" current count:"+count+" total:"+clueDataList.size());
                    conn.commit(); // 提交当前批次
                }
            }
            
            // 执行剩余的批处理
            pstmt.executeBatch();
            conn.commit(); // 提交最后一批数据
            
            System.out.println("all data insert into db");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}
    
    
    /**
     * 使用命名参数的替代方法（如果需要）
     */
    public void batchSaveClueDataWithNamedParams(List<ClueData> clueDataList) {
        if (clueDataList == null || clueDataList.isEmpty()) {
            return;
        }
        
        // 创建参数源数组
        SqlParameterSource[] batchParams = new SqlParameterSource[clueDataList.size()];
        
        for (int i = 0; i < clueDataList.size(); i++) {
            ClueData clueData = clueDataList.get(i);
            MapSqlParameterSource params = new MapSqlParameterSource();
            if(i == 0) {
        		System.out.println("count=0 data:"+gson.toJson(clueData));
        	}
            
            // 设置命名参数
            params.addValue("clueDetailJson", gson.toJson(clueData.clue_detail));
            params.addValue("submitTimes", clueData.submit_times);
            params.addValue("isCollect", clueData.is_collect);
            params.addValue("isGrant", clueData.is_grant);
            params.addValue("queryClueCardInfoJson", gson.toJson(clueData.query_clue_card_info));
            params.addValue("productDiagnoseResultJson", gson.toJson(clueData.product_diagnose_result));
            params.addValue("autoSubmitTaskJson", gson.toJson(clueData.auto_submit_task));
            params.addValue("cateQualificationOpen", clueData.cate_qualification_open);
            params.addValue("hasCateQualification", clueData.has_cate_qualification);
            params.addValue("wordClueIndicatorInfoJson", gson.toJson(clueData.word_clue_indicator_info));
            params.addValue("categoryClueExtraInfoJson", gson.toJson(clueData.category_clue_extra_info));
            params.addValue("hotSaleProductsJson", gson.toJson(clueData.hot_sale_products));
            params.addValue("clueIndicatorJson", gson.toJson(clueData.clue_indicator));
            params.addValue("clueCollectInfoJson", gson.toJson(clueData.clue_collect_info));
            params.addValue("batchDate", new Timestamp(clueData.batch_date.getTime()));
            params.addValue("keyword", clueData.getKeyword());
            
            batchParams[i] = params;
        }
        
        // 执行批量插入
        namedParameterJdbcTemplate.batchUpdate(BATCH_INSERT_OR_UPDATE_SQL, batchParams);
        
        System.out.println("All data inserted into db using named parameters, total: " + clueDataList.size());
    }
    
}
