package org.elasticsearch.plugins.storedprocedure;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.storedprocedure.common.Utils;
import org.elasticsearch.plugins.storedprocedure.service.StoredProcedureService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;



public class StoredProcedureAction extends BaseRestHandler {

	private StoredProcedureService storedProcedureService;
	
	@Inject
	public StoredProcedureAction(Settings settings, Client client, RestController controller) {
		super(settings, client);
		storedProcedureService = new StoredProcedureService(client);				
		controller.registerHandler(PUT, "/{index}/{type}/_storedprocedure/{storedSearchName}", this);//save
		controller.registerHandler(GET, "/{index}/{type}/_storedprocedure/{storedSearchName}", this);//op=run|parse
		controller.registerHandler(DELETE, "/{index}/{type}/_storedprocedure/{storedSearchName}", this);//delete		
		controller.registerHandler(GET, "/{index}/{type}/{id}/_storedprocedure/{storedSearchName}", this);
	}

	public void handleRequest(RestRequest request, RestChannel channel) {
		XContentRestResponse res = null;
		if (request.method() == RestRequest.Method.PUT){
			res = storedProcedureService.saveMeta(request);		
		}else if (request.method() == RestRequest.Method.DELETE){
			//delete
			res = storedProcedureService.deleteMeta(request);
		}else if (request.method() == RestRequest.Method.GET){
			String op = request.param("op");
			if (null == op){
				//get meta
				res = storedProcedureService.getMeta(request);
			}else if (null != op && "run".equalsIgnoreCase(op)){
				//run
				if ("search".equalsIgnoreCase(getProcedureType(request))){
					res = storedProcedureService.executeSearch(request);
				}else{
					res = storedProcedureService.executeUpdate(request);
				}				
			}else if (null != op && "parse".equalsIgnoreCase(op)){
				//parse
			}else{
				//illegal
				res = Utils.buildErrorResponse(request, "unsupported operation", null);
			}
		}		
		channel.sendResponse(res);
	}
	
	private String getProcedureType(RestRequest request){
    	String id = request.param("id");
    	if (id == null || "_storedprocedure".equalsIgnoreCase(id)){
    		return "search";
    	}else{
    		return "update";
    	}
    }
	

}
