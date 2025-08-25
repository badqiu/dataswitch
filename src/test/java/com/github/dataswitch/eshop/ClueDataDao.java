package com.github.dataswitch.eshop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.google.gson.Gson;

/**
 * 抖店-商机中心-爬虫数据批量插入实现
 * 
drop table clue_data;CREATE

 TABLE clue_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    clue_detail JSON,
    submit_times INT,
    is_collect BOOLEAN,
    is_grant BOOLEAN,
    query_clue_card_info JSON,
    product_diagnose_result JSON,
    auto_submit_task JSON,
    cate_qualification_open BOOLEAN,
    has_cate_qualification BOOLEAN,
    word_clue_indicator_info JSON,
    category_clue_extra_info JSON,
    hot_sale_products JSON,
    clue_indicator JSON,
    clue_collect_info JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_date DateTime
);

 */
public class ClueDataDao {
    private static final String DB_URL = "jdbc:mysql://107.173.159.122:3306/eshop_test?useInformationSchema=true&nullDatabaseMeansCurrent=true&useUnicode=true&characterEncoding=utf8&allowMultiQueries=true";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "pwd_dkSjdjkJfffVVjd23hf82kdsfusdfhjhdhfjijfjfuuHFhdfdjysywO";
    
    // 批量插入的SQL语句，与单条插入相同
    private static final String BATCH_INSERT_SQL = "INSERT INTO clue_data (clue_detail, submit_times, is_collect, is_grant, "
                   + "query_clue_card_info, product_diagnose_result, auto_submit_task, "
                   + "cate_qualification_open, has_cate_qualification, word_clue_indicator_info, "
                   + "category_clue_extra_info, hot_sale_products, clue_indicator, clue_collect_info,batch_date) "
                   + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    // Gson实例作为静态变量，避免重复创建
    private static final Gson gson = new Gson();

    int batchSize = 500;
    /**
     * 批量保存ClueData列表
     * @param clueDataList 要保存的ClueData对象列表
     */
    public void batchSaveClueData(List<ClueData> clueDataList) {
        if (clueDataList == null || clueDataList.isEmpty()) {
            return; // 如果列表为空，直接返回
        }
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(BATCH_INSERT_SQL)) {
            
            // 关闭自动提交，手动控制事务
            conn.setAutoCommit(false);
            
            int count = 0;
            for (ClueData clueData : clueDataList) {
                // 设置参数
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
                pstmt.setObject(15, clueData.batch_date);
                
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
            
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
