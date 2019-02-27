package com.nr.expertservices.tibco.ems.monitor;

import java.util.HashMap;
import java.util.Iterator;

/*
 * Program to pull metric data from TIBCO EMS
 * New Relic, Inc.  ALL RIGHTS RESERVED
 * Requires TIBCO libraries linked as tibjms.jar, tibjmsadmin.jar, jms-2.0.jar
 * Also the newrelic metrics_publish.2.0.1.jar
 * Created 2015-12-09
 * Modification History:
 * 
 */

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.AgentFactory;
import com.newrelic.metrics.publish.configuration.Config;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.newrelic.metrics.publish.util.Logger;
import com.tibco.tibjms.admin.TibjmsAdmin;
import com.tibco.tibjms.admin.TibjmsAdminException;

public class EMSMonitorFactory extends AgentFactory {
	
	private static final int DEFAULT_PORT = 7222;
	private static final Logger logger = Logger.getLogger(EMSMonitorFactory.class);

	@Override
	public Agent createConfiguredAgent(Map<String, Object> properties) throws ConfigurationException {
		logger.info("EMS Monitor version 3.0.2");
		String name = (String) properties.get("name");
		String host = (String) properties.get("host");
		Long port = (Long) properties.get("port");
		if(port == null) {
			port = new Long(DEFAULT_PORT);
		}
		String username = (String) properties.get("username");
		String password = (String) properties.get("password");
		Boolean encryptPassword = (Boolean) properties.get("encryptPassword");

		if (encryptPassword)
			try {
				String unmangledPassword = TibjmsAdmin.unmanglePassword(password);
				password = unmangledPassword;
			} catch (TibjmsAdminException e) {
				e.printStackTrace();
			}
		
		logger.info("Creating instance for ",name);
		
		EMSServer ems = new EMSServer(name, host, port.intValue(), username, password);

		ems.setFlagIncludeDynamicQueues ((Boolean) properties.get("includeDynamicQueues"));
		logger.info("Include Dynamic Queues? ", ems.getFlagIncludeDynamicQueues());
		JSONArray qIgnores = (JSONArray) properties.get("queueIgnores");
		for(int i=0;i<qIgnores.size();i++) {
			JSONObject obj = (JSONObject) qIgnores.get(i);
			String regEx = (String) obj.get("qIgnoreRegEx");
			ems.addToQueueIgnores(regEx);
			logger.info("Add to queue ignores ",regEx);
		}

		ems.setFlagIncludeDynamicTopics ((Boolean) properties.get("includeDynamicTopics"));
		logger.info("Include Dynamic Topics? ", ems.getFlagIncludeDynamicTopics());
		JSONArray tIgnores = (JSONArray) properties.get("topicIgnores");
		for(int i=0;i<tIgnores.size();i++) {
			JSONObject obj = (JSONObject) tIgnores.get(i);
			String regEx = (String) obj.get("tIgnoreRegEx");
			ems.addToTopicIgnores(regEx);
			logger.info("Add to topic ignores ",regEx);
		}
		
		EMSMonitor agent = new EMSMonitor(ems);

	    boolean sendToInsights = false;
	    Object insightsObj = Config.getValue("insights");
	    if (((insightsObj != null) && (Boolean.class.isInstance(insightsObj))) || (String.class.isInstance(insightsObj))) {
	      if (String.class.isInstance(insightsObj))
	        sendToInsights = Boolean.parseBoolean((String)insightsObj);
	      else if (Boolean.class.isInstance(insightsObj)) {
	        sendToInsights = ((Boolean)insightsObj).booleanValue();
	      }
	      agent.setSendToInsights(sendToInsights);
	    }

	    String apiKey = (String)Config.getValue("insights-key");
	    Long acctId = (Long)Config.getValue("account-id");
	    if ((sendToInsights) && (apiKey != null) && (acctId != null)) {
	      agent.createInsightPost(apiKey, acctId.intValue());
	    }
	    
	    JSONArray labelString = (JSONArray)Config.getValue("labels");
	    logger.debug("Labels ", labelString.toJSONString());
	    
		HashMap <String,String> labelInstances = new HashMap <String,String>();
	    for(int i=0; i<labelString.size(); i++) {
	    	JSONObject thisLabel = (JSONObject) labelString.get(i);
	    	Iterator<?> keys = thisLabel.keySet().iterator();
	    	while(keys.hasNext()) {
	    		String thisKey = (String) keys.next().toString();
	    		String thisValue = (String) thisLabel.get(thisKey);
	    		labelInstances.put(thisKey, thisValue);
	    		}
	    	}
	    if(!labelInstances.isEmpty()) ems.setLabel(labelInstances);
	    
		return agent;
	}

}
