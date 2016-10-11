package com.github.dataswitch.output;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils;

import com.github.dataswitch.serializer.Serializer;

public class HttpClientOutput  extends BaseOutput implements Output {

	private Serializer serializer  = null;
	
	public Serializer getSerializer() {
		return serializer;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
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

	@Override
	public void close() throws IOException {
	}
	
	@Override
	public void writeObject(Object obj) {
		
	}
	
}
