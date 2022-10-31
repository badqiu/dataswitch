package com.github.dataswitch.output;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.github.dataswitch.serializer.TxtSerializer;
import com.github.dataswitch.util.MapUtil;

public class HttpOutputTest {

	HttpOutput output = new HttpOutput();
	@Test
	public void test() throws Exception {
//		String result = sendPost("http://localhost:8080/userdata/importData.do?username=yuminxiong&password=iXZfu38dfdo20M8Odw9eJR5dfeQ2ogy4o0Oet4G&importMode=append&appId=lj&module=master","badqiu,data1\njane,data2\nxm,data3");
//		System.out.println(result);
		
		output.setUrl("http://localhost:8080/userdata/importData.do?username=yuminxiong&password=iXZfu38dfdo20M8Odw9eJR5dfeQ2ogy4o0Oet4G&importMode=append&appId=lj&module=master");
		TxtSerializer txt = new TxtSerializer();
		txt.setColumnSplit(",");
		txt.setColumns("account,data");
		output.setSerializer(txt);
		output.open(null);
		for(int i = 0; i < 100; i++) {
			output.writeObject(MapUtil.newMap("account","a"+i,"data","data"+i));
		}
		output.close();
		
//		byte[] body = "badqiu,data1\njane,data2\nxm,data3".getBytes();
//		String response = sendPostByHttpClient("http://localhost:8080/userdata/importData.do?username=yuminxiong&password=iXZfu38dfdo20M8Odw9eJR5dfeQ2ogy4o0Oet4G&importMode=append&appId=lj&module=master",body);
//		System.out.println(response);
	}
	
	
	public String sendPostByHttpClient(String url,byte[] body) throws Exception {
		MultiThreadedHttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams params = new HttpConnectionManagerParams();
		params.setDefaultMaxConnectionsPerHost(300);
		params.setMaxTotalConnections(500);
		httpConnectionManager.setParams(params);
		
		HttpClient httpClient = new HttpClient(httpConnectionManager);
		
		return doExecuteRequest(httpClient, url, body);
	}
	
	protected String doExecuteRequest(HttpClient httpClient,String url,byte[] body) throws Exception {
		PostMethod postMethod = new PostMethod(url);
		try {
			postMethod.setRequestEntity(new ByteArrayRequestEntity(body, "application/json"));
			httpClient.executeMethod(postMethod);
			if (postMethod.getStatusCode() >= 300) {
				throw new HttpException(
						"Did not receive successful HTTP response: status code = " + postMethod.getStatusCode() +
						", status message = [" + postMethod.getStatusText() + "]");
			}
			InputStream responseBody = postMethod.getResponseBodyAsStream();
			return IOUtils.toString(responseBody);
		}
		finally {
			// Need to explicitly release because it might be pooled.
			postMethod.releaseConnection();
		}
	}
	
	
	/**  
     * 向指定URL发送POST方法的请求  
     *   
     * @param url  
     *            发送请求的URL  
     * @param param  
     *            请求参数，请求参数应该是name1=value1&name2=value2的形式。  
     * @return URL所代表远程资源的响应  
	 * @throws MalformedURLException 
     */  
    public static String sendPost(String url, String param) throws Exception {  
    	System.out.println("body:"+param);
    	byte[] body = param.getBytes();
        OutputStream out = null;  
        InputStream in = null;  
        try {  
            URL realUrl = new URL(url);  
            // 打开和URL之间的连接  
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            
            // 发送POST请求必须设置如下两行  
            conn.setDoOutput(true);  
//            conn.setDoInput(true);  
            conn.setInstanceFollowRedirects( false );
            conn.setUseCaches( false );
            
            conn.setRequestMethod("POST"); 
            conn.setRequestProperty("accept", "*/*");  
            conn.setRequestProperty("connection","Keep-Alive");  
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");
//            conn.setRequestProperty( "Content-Length", Integer.toString(body.length));
            conn.setRequestProperty( "Content-Type", "application/byte"); 
            conn.setRequestProperty( "charset", "utf-8");
            
            // 获取URLConnection对象对应的输出流  
            out = conn.getOutputStream();
            // 发送请求参数  
            out.write(body);
            // flush输出流的缓冲  
            out.flush();  
            
            // 定义BufferedReader输入流来读取URL的响应  
            in = conn.getInputStream();
            return IOUtils.toString(in);

        // 使用finally块来关闭输出流、输入流  
        }finally {  
        	IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }  
    } 

}
