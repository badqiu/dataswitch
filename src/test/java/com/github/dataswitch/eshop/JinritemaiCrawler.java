package com.github.dataswitch.eshop;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JinritemaiCrawler {



	static ClueDataDao clueDataDao = new ClueDataDao();


    // 商机中心关键词URL
    private static final String API_URL = "https://fxg.jinritemai.com/api/commop/business_chance_center/clue/common/real_time_list";
    
    private static final String[] USER_AGENTS = {
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    };
    private static final Random random = new Random();

    // 请求头配置
    private static final Map<String, String> HEADERS = new HashMap<String, String>() {{
        put("accept", "application/json, text/plain, */*");
        put("accept-language", "zh-CN,zh;q=0.9");
        put("content-type", "application/json");
        put("origin", "https://fxg.jinritemai.com");
        put("priority", "u=1, i");
        put("referer", "https://fxg.jinritemai.com/ffa/bu/NewBusinessCenter?btm_ppre=a0.b0.c0.d0&btm_pre=a2427.b76571.c902327.d871297&btm_show_id=fb6c1550-0137-4a56-b804-26a6bad31a4a");
        put("sec-ch-ua", "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"");
        put("sec-ch-ua-mobile", "?0");
        put("sec-ch-ua-platform", "\"Windows\"");
        put("sec-fetch-dest", "empty");
        put("sec-fetch-mode", "cors");
        put("sec-fetch-site", "same-origin");
        put("x-secsdk-csrf-token", "000100000001fab168c28445d7adcccef60e928422222dd6d8f8190b09794c32b5bc4abc7c98185eb89d5815d12c");
    }};

    // 登录安全认证，Cookie配置（从CURL中提取）
    private static final String COOKIE_STRING = "is_staff_user=false; _tea_utm_cache_486645={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_2018={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_3813={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_1215={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_518298={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_499020={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; qc_tt_tag=0; passport_mfa_token=CjfT3TEMc1E%2BOXMT7vnhvDRlX5oQ4sbJHYGZDltxUWNuNKCzlSNrx0aQgCAtDt3iVBv6ltiyW5eeGkoKPAAAAAAAAAAAAABPWhgWxy3sTJQl73ZcV1Ih5n2Jd5ypP4b17elSFmMD%2B9Ka7ps9uTQ8x7MfzIYMjP1urBCVuvkNGPax0WwgAiIBA7job3w%3D; SHOP_ID=227753980; PIGEON_CID=598607441699485; s_v_web_id=verify_mefx95tu_N5rqXJUL_hBvf_4fcj_9Sqz_U9sKlJeb1WLV; passport_csrf_token=157f8b70078bd0716331282928ff020f; passport_csrf_token_default=157f8b70078bd0716331282928ff020f; _tea_utm_cache_1574={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_409608={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_481911={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_7418={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_6549={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; _tea_utm_cache_354423={%22utm_source%22:%22qianchuan-origin-entrance%22%2C%22utm_medium%22:%22doudian-pc%22%2C%22utm_campaign%22:%22newshoptask%22%2C%22utm_term%22:%22tg%22}; gfkadpd=4272,23756; Hm_lvt_b6520b076191ab4b36812da4c90f7a5e=1754502077,1755007105,1755190737,1755942276; zsgw_business_data=%7B%22uuid%22%3A%22f6b75e20-5adf-458c-8790-34b74c4dfcfc%22%2C%22platform%22%3A%22pc%22%2C%22source%22%3A%22bing.sem.1%22%7D; gd_random=eyJtYXRjaCI6dHJ1ZSwicGVyY2VudCI6MC4wNTczNzcxNjgwNzAyNDYyOX0=.au8CFQKznph2oRUJN8uYu0rFNIwZtnG/8hMvQ0/n6bQ=; fxg_guest_session=eyJhbGciOiJIUzI1NiIsInR5cCI6InR5cCJ9.eyJndWVzdF9pZCI6IkNnWUlBU0FIS0FFU1BnbzhhMHNBcVNYeGI4WTVPRnBJaHlhOGVaWE9VMDVKYjZ1UFRPOFBmbVRPWTdZZXpJaWNtekNJaUN4RHFZSDVXdGhWUXBRenQranY5Q1N6YUExN0dnQT0iLCJpYXQiOjE3NTU5NDc5OTMsIm5iZiI6MTc1NTk0Nzk5MywiZXhwIjoxNzU3MjQzOTkzfQ%3D%3D.98d381a3814eff18f2a5c0e5068f82951b839da5088cf430ffb6cd697908b90f; tt_scid=Z9nuLc8EYRIvWe5YTQUhqG5RbyQXf449-wpbjdUzA2tzbXeXGEZJNvlcKFmtz0HT6e6f; odin_tt=6d4819cb1868909c24c0b21ddb92a8c4b08e3b47e8b1407d63d8d0f7a4d791e47fb9c2c86eaf467ead6d0171e3d4ad617584ad9fa45f62cd61484bf11b98b6c5; passport_auth_status=da6aec77bbde8f1e8621eaea4f8e7a2e%2C226b565412fb6b46085e976618930368; passport_auth_status_ss=da6aec77bbde8f1e8621eaea4f8e7a2e%2C226b565412fb6b46085e976618930368; uid_tt=b01b67993d5b74fb48c05e6b90dcfde1; uid_tt_ss=b01b67993d5b74fb48c05e6b90dcfde1; sid_tt=a5a6736762cf7ef2754c1120a85b111f; sessionid=a5a6736762cf7ef2754c1120a85b111f; sessionid_ss=a5a6736762cf7ef2754c1120a85b111f; session_tlb_tag=sttt%7C19%7CpaZzZ2LPfvJ1TBEgqFsRH__________0eKqKuL08pEi39e291v-rlL5tPUxDr14iH8tunj5hZag%3D; ucas_c0=CkEKBTEuMC4wEKSIgcL-_ejUaBjmJiCoioCIvKzoAyiwITCdxbC_4o2IAUD4x6bFBkj4--LHBlCpvLr449z9nWhYbhIUxFMh62IVWnqbnZVlqyfpg3_F-ok; ucas_c0_ss=CkEKBTEuMC4wEKSIgcL-_ejUaBjmJiCoioCIvKzoAyiwITCdxbC_4o2IAUD4x6bFBkj4--LHBlCpvLr449z9nWhYbhIUxFMh62IVWnqbnZVlqyfpg3_F-ok; PHPSESSID=008704d6a8c6aca914500c0fbcf4a4e7; PHPSESSID_SS=008704d6a8c6aca914500c0fbcf4a4e7; ecom_gray_shop_id=227753980; sid_guard=a5a6736762cf7ef2754c1120a85b111f%7C1755948038%7C5184000%7CWed%2C+22-Oct-2025+11%3A20%3A38+GMT; sid_ucp_v1=1.0.0-KGRiOGVkOTE5MzA0MDQ0NTEwZjA1OWNmMDViZWViYWY3NmZjYWJhZjUKGQidxbC_4o2IARCGyKbFBhiwISAMOAZA9AcaAmxmIiBhNWE2NzM2NzYyY2Y3ZWYyNzU0YzExMjBhODViMTExZg; ssid_ucp_v1=1.0.0-KGRiOGVkOTE5MzA0MDQ0NTEwZjA1OWNmMDViZWViYWY3NmZjYWJhZjUKGQidxbC_4o2IARCGyKbFBhiwISAMOAZA9AcaAmxmIiBhNWE2NzM2NzYyY2Y3ZWYyNzU0YzExMjBhODViMTExZg; COMPASS_LUOPAN_DT=session_7541734689871036715; BUYIN_SASID=SID2_7541737216373047561; csrf_session_id=2552af04012be8f34e887ce39b415e25; Hm_lvt_55b6f6890a6937842cef785d95ea99d7=1755196604,1755942294,1756101542; Hm_lpvt_55b6f6890a6937842cef785d95ea99d7=1756101542; HMACCOUNT=3C7379D5442BFF5F; Hm_lvt_ed0a6497a1fdcdb3cdca291a7692408d=1755195696,1755942294,1756101542; Hm_lpvt_ed0a6497a1fdcdb3cdca291a7692408d=1756101542; Hm_lvt_f0c7a7e443258d32622a2a8a3ded1d8f=1755196604,1755942294,1756101542; Hm_lpvt_f0c7a7e443258d32622a2a8a3ded1d8f=1756101542; Hm_lvt_729f63f2a2cf56cd38fff0220c787b4a=1755195696,1755942294,1756101543; Hm_lpvt_729f63f2a2cf56cd38fff0220c787b4a=1756101543; ttwid=1%7CZpmsO74XHnK8WfVJMHQpVBxiOO6eTkTUv4aNomcTjOY%7C1756101575%7Cbffd8ac31b56844b91a1d6938bc3f20936ec7c0111b876406603ebcd225cfac8; ffa_goods_ewid=e0b7c07a5d8e03828fc91a45320cdc83; ffa_goods_seraph_did=undefined"; // 完整Cookie字符串
    
    private static int pageSize = 9000;
    
    public static void main(String[] args) {
        
//        int totalPages = getTotalPages(); // 获取总页数
        int totalPages = 1;
        System.out.println("总页数: " + totalPages);
        
        Date batchDate = extracteDate(new Date(),"yyyyMMddHH");
        // 分页爬取
        for (int currentPage = 1; currentPage <= totalPages; currentPage++) {
            try {
                String jsonResponse = fetchApiData(currentPage);
                System.out.println("正在爬取第 " + currentPage + " 页，共 " + totalPages + " 页");
//                System.out.println("正在爬取第 " + currentPage + " 页，共 " + totalPages + " 页 jsonResponse:"+jsonResponse);
                List<ClueData> clues = parseResponse(jsonResponse);
                for(ClueData c : clues) {
                	c.setBatch_date(batchDate);
                }
                clueDataDao.batchSaveClueData(clues);
                
                sleepRandomSecondsForBlock();
            } catch (Exception e) {
                System.err.println("爬取第 " + currentPage + " 页时出错: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

	private static Date extracteDate(Date date,String format) {
		return DateUtils.parseDate(DateFormatUtils.format(date,format));
	}

	private static void sleepRandomSecondsForBlock() throws InterruptedException {
		// 随机延迟防止被封
		int delay = 2000 + random.nextInt(3000); // 2-5秒随机延迟
		TimeUnit.MILLISECONDS.sleep(delay);
	}

    /**
     * 获取总页数（每页30条）
     */
    private static int getTotalPages() {
        try {
            String jsonResponse = fetchApiData(1);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            int total = rootNode.path("total").asInt();
            return (int) Math.ceil((double) total / pageSize);
        } catch (Exception e) {
            throw new RuntimeException("getTotalPages error,url:",e);
        }
    }

    static ObjectMapper objectMapper = new ObjectMapper();
    /**
     * 发送API请求
     */
    private static String fetchApiData(int page) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(API_URL);
            
            // 设置请求头
            HEADERS.forEach(request::setHeader);
            request.setHeader("User-Agent", USER_AGENTS[random.nextInt(USER_AGENTS.length)]);
            request.setHeader("Cookie", COOKIE_STRING);
            
            // 构建请求体
            String jsonBody = buildRequestBody(page);
            request.setEntity(new StringEntity(jsonBody, "UTF-8"));
            
            // 执行请求
            HttpResponse response = httpClient.execute(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            Map responseMap = objectMapper.readValue(responseBody, Map.class);
            String code = String.valueOf(responseMap.get("code"));
            if(!"0".equals(code)) {
            	throw new RuntimeException(API_URL + " response error:"+responseBody);
            }
			return responseBody;
        }
    }

    /**
     * 构建请求体
     */
    private static String buildRequestBody(int currentPage) {
        return String.format(
            "{\"condition\":{\"clue_info\":\"\",\"recently_created\":false,\"sort\":{\"sort_direction\":1,\"sort_field\":\"MATCH_DEGREE\"},\"category_qualification\":false,\"category_clue_auto_submit\":false,\"tag_id_list\":[],\"profit_id_list\":[],\"clue_attr_key_list\":[],\"benefit_crowd_group\":[],\"benefit_content_type\":[],\"attr_values\":[],\"hit_clue_label_ext\":true,\"show_new_supply_link\":true,\"include_hot_sales_products\":true},\"clue_type\":\"\",\"clue_type_new\":11,\"page\":{\"page_size\":"+pageSize+",\"current\":%d},\"terminal_type\":0,\"source\":\"business_center\"}",
            currentPage
        );
    }

    public static class DouyinResponse {
    	public List<ClueData> data;
    	public Long total;
    	public Long code;
    	public Map base_resp;
    }
    
    /**
     * 解析JSON响应
     */
    private static List<ClueData> parseResponse(String jsonResponse) throws JsonProcessingException {
        List<ClueData> clues = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        DouyinResponse responseData = objectMapper.readValue(jsonResponse, DouyinResponse.class);
        return responseData.data;
    }



   
}