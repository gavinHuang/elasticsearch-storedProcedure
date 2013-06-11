package org.elasticsearch.plugins.storedprocedure.service;

import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.storedprocedure.common.Config;
import org.elasticsearch.plugins.storedprocedure.common.Utils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;


public class StoredProcedureService {

    private static final ESLogger log = Loggers.getLogger(StoredProcedureService.class);      
    
    private Client client;
    
    public StoredProcedureService(Client client){
    	this.client = client;
    }
    
    public GetResponse put(String index, String type, String id, Map<String, Object> content){
    	GetResponse getResponse = null;
    	try {
    		//save
			client.prepareIndex(index, type, id).setSource(content).execute().actionGet();
			
			//verify
			getResponse = get(index, type, id);
			if (!getResponse.exists()){
				Thread.sleep(1000);//wait another second and try again				
				getResponse = get(index, type, id);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			log.error("error when trying to save stored procedure", e);
		}
    	return getResponse;
    }
        
    public GetResponse get(String index, String type, String id){
    	return client.prepareGet(index, type, id)
    			.execute()
    			.actionGet();
    }
        
    public DeleteResponse delete(String index, String type, String id){
    	return client.prepareDelete(index, type, id).execute().actionGet();
    }
   
    public XContentRestResponse saveMeta(RestRequest request){
    	XContentRestResponse response = null;
    	String index = request.param("index");
    	String type = request.param("type");
    	String name = request.param("storedSearchName");
    	String storedProcedureId = Utils.buildId(index, type, name);
		try {
			Map<String, Object> meta = new HashMap<String, Object>();
			meta.put("content", request.content().toUtf8());
			GetResponse getResponse = put(Config.StoredProcedureIndex, Config.StoredProcedureType, storedProcedureId, meta);
			if (getResponse == null || !getResponse.exists()){
				//error
				response = Utils.buildErrorResponse(request, "save stored procedure failed", null);
			}
			XContentBuilder xb = RestXContentBuilder.restContentBuilder(request);
			Map<String, Object> source = getResponse.sourceAsMap();
			//Map<String, Object> content = XContentFactory.xContent(XContentType.JSON).createParser(source).mapAndClose();
			//xb.startObject()
			xb.map(source);    		
			//xb.endObject();
			response = new XContentRestResponse(request, RestStatus.OK, xb);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("error when trying to save stored procedure", e);
			response = Utils.buildErrorResponse(request, "error when trying to save stored procedure", e);
		}    	
    	return response;
    }
    
    public XContentRestResponse getMeta(RestRequest request){
    	XContentRestResponse response = null;
    	XContentBuilder xb = null;
    	String index = request.param("index");
    	String type = request.param("type");
    	String name = request.param("storedSearchName");
    	String storedProcedureId = Utils.buildId(index, type, name);

		try {
			GetResponse getResponse = this.get(Config.StoredProcedureIndex, Config.StoredProcedureType, storedProcedureId);
			if (getResponse != null && getResponse.exists()){
				xb = RestXContentBuilder.restContentBuilder(request);
				//xb.startObject();
				//xb = getResponse.toXContent(xb, null);
				//xb.endObject();
				Map<String, Object> source = getResponse.sourceAsMap();
				xb.map(source);
				response = new XContentRestResponse(request, RestStatus.OK, xb);
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.error("get stored procedure failed", e);
			response = Utils.buildErrorResponse(request, "get stored procedure failed", e);
		}	
		return response;
    }    
  
    public XContentRestResponse deleteMeta(RestRequest request){
    	XContentRestResponse response = null;
    	String index = request.param("index");
    	String type = request.param("type");
    	String name = request.param("storedSearchName");
    	String storedProcedureId = Utils.buildId(index, type, name);
    	DeleteResponse deleteResponse = this.delete(Config.StoredProcedureIndex, Config.StoredProcedureType, storedProcedureId);
    	if (deleteResponse != null){
    		try {
    			XContentBuilder xb = RestXContentBuilder.restContentBuilder(request);
    			xb.startObject()
    				.field("ok", true)
    				/*.field("found",deleteResponse.isNotFound())
                    .field("_index", deleteResponse.index())
                    .field("_type", deleteResponse.type())
                    .field("_id", deleteResponse.id())
                    .field("_version", deleteResponse.version())*/
    				.field("acknowlege", true)
                .endObject();
    			response = new XContentRestResponse(request, RestStatus.OK, xb);
    		} catch (IOException e) {
    			e.printStackTrace();
    			log.error("delete stored procedure failed", e);
    			response = Utils.buildErrorResponse(request, "unsupported operation: not in 'search' or 'update'", e);
    		}
    	}else{
    		response = Utils.buildErrorResponse(request, "delete stored procedure failed", null);
    	}
		
    	return response;
    }
    
    public XContentRestResponse getParsedMeta(RestRequest request){
    	XContentRestResponse response = null;
    	String meta = this.getParsedStoredProcedure(request);
    	if (meta != null){
    		try {
				XContentBuilder xb = RestXContentBuilder.restContentBuilder(request);
				Map<String, Object> content = XContentFactory.xContent(XContentType.JSON).createParser(meta).mapAndClose();
				xb.map(content);
				response = new XContentRestResponse(request, RestStatus.OK, xb);
			} catch (IOException e) {
				e.printStackTrace();
				log.error("get parsed stored procedure failed", e);
				response = Utils.buildErrorResponse(request, "get parsed stored procedure failed", e);
			}    		
    	}else{
    		response = Utils.buildErrorResponse(request, "stored procedure not found", null);
    	}
    	return response;
    }
    
    public String getParsedStoredProcedure(RestRequest request){
    	String response = null;
    	String index = request.param("index");
    	String type = request.param("type");
    	String name = request.param("storedSearchName");
    	String storedProcedureId = Utils.buildId(index, type, name);
    	GetResponse getResponse = this.get(Config.StoredProcedureIndex, Config.StoredProcedureType, storedProcedureId);
    	if (getResponse.exists()){
    		String requestBody = (String)getResponse.getSource().get("content");
    		Map<String, Object> parameters = this.retrieveParametersFromUrl(request);
    		response = this.processTemplate(storedProcedureId, requestBody, parameters);
    	}    	
		return response;
    }
    
    public XContentRestResponse executeUpdate(RestRequest request){
    	String index = request.param("index");
    	String type = request.param("type");
    	String documentId = request.param("id");
    	XContentRestResponse response = null;
    	String requestBody = this.getParsedStoredProcedure(request);

    	if (requestBody != null){
    		response = Utils.buildErrorResponse(request, "stored procedure not exits!", null);
    	}else{
    		try {
    			UpdateRequest updateRequest = new UpdateRequest(index, type, documentId);
    			Map<String, Object> content = XContentFactory.xContent(XContentType.JSON)
                        .createParser(requestBody).mapAndClose();
                if (content.containsKey("script")) {
                    updateRequest.script(content.get("script").toString());
                }
                if (content.containsKey("lang")) {
                    updateRequest.scriptLang(content.get("lang").toString());
                }
                if (content.containsKey("params")) {
                    updateRequest.scriptParams((Map<String, Object>) content.get("params"));
                }
                UpdateResponse updateResponse = client.update(updateRequest).actionGet();
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                builder.startObject()
                        .field("ok", true)
                        .field("_index", updateResponse.index())
                        .field("_type", updateResponse.type())
                        .field("_id", updateResponse.id())
                        .field("_version", updateResponse.version());
                if (updateResponse.matches() != null) {
                    builder.startArray("matches");
                    for (String match : updateResponse.matches()) {
                        builder.value(match);
                    }
                    builder.endArray();
                }
                builder.endObject();
                RestStatus status = OK;
                if (updateResponse.version() == 1) {
                    status = CREATED;
                }
                response = new XContentRestResponse(request, status, builder);
    		} catch (IOException e) {
    			e.printStackTrace();
    			log.error("execute stored procedure failed", e);
    			response = Utils.buildErrorResponse(request, "unsupported operation: not in 'search' or 'update'", e);
    		}catch (Exception e) {
            	e.printStackTrace();
            	log.error("execute stored procedure failed", e);
    			response = Utils.buildErrorResponse(request, "unsupported operation: not in 'search' or 'update'", e);
            }
    	}
    	return response;
    }
    

    public XContentRestResponse executeSearch(RestRequest request){
    	String index = request.param("index");
    	String type = request.param("type");
    	String name = request.param("storedSearchName");
    	XContentRestResponse response = null;
    	String requestBody = this.getParsedStoredProcedure(request);
    	if (requestBody == null){
			//error
			response = Utils.buildErrorResponse(request, "stored procedure not exsits", null);
		}else{
			Map<String, Object> parameters = this.retrieveParametersFromUrl(request);
			
			
			SearchRequestBuilder requestBuilder = client.prepareSearch(index);
	    	if (type!= null){
	    		requestBuilder.setTypes(type);
	    	}
	    	requestBuilder.setSource(requestBody);
	    	
	    	//other parameter that not for template
	    	int size = request.paramAsInt("size", 0);
	    	if (size > 0){
	    		requestBuilder.setSize(size);
	    	}
	    	int from = request.paramAsInt("from", 0);
	    	if (from > 0){
	    		requestBuilder.setFrom(from);
	    	}
	    	String routing = request.param("routing");
	    	if (routing != null){
	    		requestBuilder.setRouting(routing);
	    	}
	    	
	    	String searchType = request.param("searchType");
	    	if (searchType != null){
	    		requestBuilder.setSearchType(searchType);
	    	}	    	
	    	String timeout = request.param("timeout");
	    	if (timeout != null){
	    		requestBuilder.setTimeout(timeout);
	    	}    	
	    	SearchResponse searchResponse = requestBuilder.execute().actionGet();
	    	
	    	//response
	    	try {
				XContentBuilder xb = RestXContentBuilder.restContentBuilder(request);
				xb.startObject();
				xb = searchResponse.toXContent(xb, null);
				xb.endObject();
				response = new XContentRestResponse(request, RestStatus.OK, xb);
			} catch (IOException e) {
				e.printStackTrace();
				log.error("print stored procedure failed", e);
				response = Utils.buildErrorResponse(request, "print stored procedure result failed", e);
			}
		}
		return response;
    }
    
    
    public Map<String, Object> retrieveParametersFromUrl(RestRequest request){
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String[] qp = request.paramAsStringArray("p", null);
    	if (qp != null){
    		for (String queryParameterNameValuePair : qp){
    			 int start = queryParameterNameValuePair.indexOf(":");
    	         if (start > 0) {
    	             final String queryParameterName = queryParameterNameValuePair.substring(0, start);
    	             final String queryParameterValue = queryParameterNameValuePair.substring(start + 1);
    	             if (parameters.containsKey(queryParameterName)){
    	            	 //already exists, get it and make it a list
    	            	 Object value = parameters.get(queryParameterName);
    	            	 if (value instanceof String){
    	            		 List<String> list = new LinkedList<String>();
    	            		 list.add((String)value);
    	            		 list.add(queryParameterValue);
    	            		 parameters.put(queryParameterName, list);
    	            	 }else if (value instanceof List){
    	            		 List<String> list = (List<String>)value;
    	            		 list.add(queryParameterValue);
    	            		 parameters.put(queryParameterName, list);
    	            	 }else{
    	            		 log.warn("unexpected type of parameter:{}", queryParameterName);
    	            	 }
    	             }else{
    	            	 parameters.put(queryParameterName, queryParameterValue);
    	             }
    	         } 
    		}
    	}
    	return parameters;
    }
    
    public String processTemplate(String templateId, String templateSource, Map<String, Object> parameters){
    	StringTemplateLoader stringTemplateLoader = new StringTemplateLoader();
    	stringTemplateLoader.putTemplate(templateId, templateSource);
    	Configuration configuration = new Configuration();
    	configuration.setTemplateLoader(stringTemplateLoader);
    	StringWriter query = new StringWriter();		
    	try {
			Template template = configuration.getTemplate(templateId);
			template.process(parameters, query);
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (TemplateException e) {
			e.printStackTrace();
		}
    	return query.toString();
    }
}