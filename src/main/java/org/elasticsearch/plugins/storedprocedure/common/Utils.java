package org.elasticsearch.plugins.storedprocedure.common;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

public class Utils {

	public static String buildId(String index, String type, String name){
    	String id = String.format("%s_%s_%s", index, type, name);
    	return id;
    }
	
	public static XContentRestResponse buildErrorResponse(RestRequest request, String message, Throwable e){
    	try {
			XContentBuilder xb = RestXContentBuilder.restContentBuilder(request);
			xb.startObject()
			.field("error", message + " : " + e)
			.field("status", 500)
			.endObject();			
			XContentRestResponse response = new XContentRestResponse(request, RestStatus.INTERNAL_SERVER_ERROR, xb);
			return response;
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	return null;
    }
}
