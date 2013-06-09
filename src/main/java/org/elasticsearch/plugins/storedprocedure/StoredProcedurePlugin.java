package org.elasticsearch.plugins.storedprocedure;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;


public class StoredProcedurePlugin extends AbstractPlugin {

	protected final ESLogger logger = Loggers.getLogger(StoredProcedurePlugin.class);
			
	public String name() {
		return "StoredProcedurePlugin";
	}

	public String description() {
		return "save your query with a name, and call your pre-saved query";
	}
	
	@Override 
	public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(StoredProcedureAction.class);
            logger.info("StoredProcedurePlugin plugined!");
        }
    }

}
