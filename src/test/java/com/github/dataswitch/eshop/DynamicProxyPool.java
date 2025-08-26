package com.github.dataswitch.eshop;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class DynamicProxyPool {
    // 存储可用代理的线程安全列表
    private static final CopyOnWriteArrayList<String> proxyPool = new CopyOnWriteArrayList<>();
    private static final String BASE_URL = "https://www.89ip.cn/index_";
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

    /**
     * 分页爬取所有代理IP
     */
    public static void crawlAllPages() {
        int page = 1;
        while (true) {
            try {
                String url = (page == 1) ? "https://www.89ip.cn/" : BASE_URL + page + ".html";
                Document doc = Jsoup.connect(url)
                    .userAgent(USER_AGENT)
                    .timeout(10000)
                    .get();
                
                // 解析IP和端口 [1,3](@ref)
                Elements ipElements = doc.select("table.layui-table tbody tr td:first-child");
                Elements portElements = doc.select("table.layui-table tbody tr td:nth-child(2)");
                
                if (ipElements.isEmpty()) break; // 无数据时终止
                
                // 提取并验证代理
                for (int i = 0; i < ipElements.size(); i++) {
                    String ip = ipElements.get(i).text().trim();
                    String port = portElements.get(i).text().trim();
                    String proxyAddress = ip + ":" + port;
                    if (validateProxy(proxyAddress)) {
                        proxyPool.add(proxyAddress);
                        System.out.println("✅ 有效代理: " + proxyAddress);
                    }
                }
                page++;
                Thread.sleep(2000); // 防止频繁请求被封IP [8](@ref)
//                break;
            } catch (IOException | InterruptedException e) {
                System.err.println("⚠️ 爬取终止于第 " + page + " 页: " + e.getMessage());
                break;
            }
        }
    }

    /**
     * 验证代理IP有效性
     */
    private static boolean validateProxy(String proxyAddress) {
        String[] parts = proxyAddress.split(":");
        if (parts.length != 2) return false;

        try {
        	URL testUrl = new URL("https://www.baidu.com");
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            HttpURLConnection conn = (HttpURLConnection) testUrl.openConnection(proxy);
//            HttpURLConnection conn = (HttpURLConnection) testUrl.openConnection();
            conn.setRequestMethod("GET");
            int timeoutMills = 500;
			conn.setConnectTimeout(timeoutMills);
            conn.setReadTimeout(timeoutMills);
            return conn.getResponseCode() == 200; // 通过百度验证 [2,5](@ref)
        } catch (Exception e) {
        	System.out.println("[WARN] proxy ip test error:"+proxyAddress+" exception:"+e);
            return false;
        }
    }

    /**
     * 从代理池随机获取一个可用代理
     */
    public static String getRandomProxy() {
        if (proxyPool.isEmpty()) return null;
        return proxyPool.get(new Random().nextInt(proxyPool.size()));
    }

    public static void main(String[] args) {
        // 步骤1: 爬取所有分页代理
        crawlAllPages();
        
        // 步骤2: 输出代理池统计信息
        System.out.println("\n=======================");
        System.out.println("✅ 代理池构建完成！");
        System.out.println("🛠️ 可用代理数量: " + proxyPool.size());
        System.out.println("🌐 随机测试代理: " + getRandomProxy());
    }
}