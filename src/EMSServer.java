package com.nr.expertservices.tibco.ems.monitor;

/*
 * Program to pull metric data from TIBCO EMS
 * New Relic, Inc.  ALL RIGHTS RESERVED
 * Requires TIBCO libraries linked as tibjms.jar, tibjmsadmin.jar, jms-2.0.jar
 * Also the newrelic metrics_publish.2.0.1.jar
 * Created 2015-12-09
 * Modification History:
 * 
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONArray;

import com.newrelic.metrics.publish.util.Logger;
import com.tibco.tibjms.admin.TibjmsAdmin;
import com.tibco.tibjms.admin.TibjmsAdminException;

public class EMSServer {
	private String host;
	private int port = 7222;
	private String username;
	private String password;
	private String getName;
	private List<String> queueIgnores;
	private List<String> topicIgnores;
	private HashMap<String, String> labelObject;
	private Boolean flagIncludeDynamicQueues;
	private Boolean flagIncludeDynamicTopics;
	private String failState = "unconnected";
	private int retries;
	private TibjmsAdmin tibConnection;
	private static final Logger logger = Logger.getLogger(EMSMonitor.class);

	
	public EMSServer(String name, String host, int port, String username, String password) {
		super();
		this.getName = name;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		queueIgnores = new ArrayList<String>();
		topicIgnores = new ArrayList<String>();
		labelObject = new HashMap<String, String>();
	}
	public String getName() {
		return getName;
	}
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	public String getEMSURL() {
		String emsURL = null;
		emsURL = "tcp://" + host + ":" + port;
		return(emsURL);
	}

	public TibjmsAdmin getConnection(EMSServer m) {
			String stateMessage="unconnected";
			TibjmsAdmin tibcoInst = null;
			logger.debug("EMSServer::getConnection: Connecting: ", m.getName());

			if( m.tibConnection != null && m.getFailState() != "failed") {
				stateMessage="connected";
				tibcoInst = m.tibConnection;
				logger.debug("EMSServer::getConnection: Currently connected: ", m.getName());
				}
			else if (m.tibConnection != null) {
				try {
					logger.debug("EMSServer::getConnection: Closing: ", m.getName());
					m.tibConnection.close();
					m.tibConnection = null;
				} catch (TibjmsAdminException e) {
					e.printStackTrace();
				}
			}

			String user = m.getUsername();
			String host = m.getEMSURL();
			String password = m.getPassword();

			if (stateMessage == "connected") {
				m.setFailState(stateMessage);				
			}
			else {			
			m.setFailState("unconnected");
			try {
				tibcoInst = new TibjmsAdmin(host, user, password);
				m.tibConnection = tibcoInst;
				m.setFailState("connected");
				m.retries = 0;
				logger.info("EMSServer::getConnection: Connected: ", m.getName());
			}
			catch (TibjmsAdminException e) {
				e.printStackTrace();
				m.retries++;
				String errText = e.toString();
				HashMap<String, Object> insightsMap = new HashMap<String, Object>();
				if(errText.contains("uthentication failed")
								|| errText.contains("uthenication failed")) {
					stateMessage="authenticationFailed";
					m.setFailState(stateMessage);
					logger.debug("EMSServer::getConnection user:",user," password:",password);
					}
				else if (errText.contains("Unable to connect to server")) {
					stateMessage="connectionFailed";
					m.setFailState(stateMessage);
					}
				else if (errText.contains("Session is closed")) {
					stateMessage="sessionFailed";
					m.setFailState(stateMessage);
					}
				else if (errText.contains("Not authorized to execute command")) {
					stateMessage="notAuthorized";
					m.setFailState(stateMessage);
					}
				else if (errText.contains("Timeout while waiting for server response")) {
					stateMessage="connectionTimeout";
					m.setFailState(stateMessage);
					}
				else if (errText.contains("Invalid store in compaction request")) {
					stateMessage="invalidDatastore";
					m.setFailState(stateMessage);
					}
				else if (errText.contains("Connection has been terminated")) {
					stateMessage="connectionTerminated";
					m.setFailState(stateMessage);
					}
/*				if(sendToInsights && insights != null) {
					try {
						insightsMap.put("Server Name", name);
						insightsMap.put("connectState", stateMessage);
						insights.post("EMSServers", insightsMap);
					} catch (Exception ei) {
						logger.error(ei, "Failed to send to Insights");
					}
				}
*/
			logger.debug("EMSServer::getConnection: Exit: ", stateMessage);
			}

		}
		return tibcoInst;
		
	}
	
	public HashMap<String, String> getLabel() {
		return labelObject;
	}
	
	public void setLabel(HashMap<String, String> labelInstance) {
		labelObject.putAll(labelInstance);
	}
	
	public String getUsername() {
		return username;
	}
	public String getPassword() {
		return password;
	}
	
	public List<String> getQueueIgnores() {
		return queueIgnores;
	}
	
	public void addToQueueIgnores(String queueIgnore) {
		queueIgnores.add(queueIgnore);
	}
	
	public void removeFromQueueIgnores(String queueIgnore) {
		queueIgnores.remove(queueIgnore);
	}
	
	public List<String> getTopicIgnores() {
		return topicIgnores;
	}
	
	public void addToTopicIgnores(String topicIgnore) {
		queueIgnores.add(topicIgnore);
	}
	
	public void removeFromTopicIgnores(String topicIgnore) {
		queueIgnores.remove(topicIgnore);
	}
	
	public void setFlagIncludeDynamicQueues(Boolean dynQValue) {
		flagIncludeDynamicQueues = dynQValue;
	}

	public Boolean getFlagIncludeDynamicQueues() {
		return flagIncludeDynamicQueues;
	}
	
	public void setFlagIncludeDynamicTopics(Boolean dynTValue) {
		flagIncludeDynamicTopics = dynTValue;
	}

	public Boolean getFlagIncludeDynamicTopics() {
		return flagIncludeDynamicTopics;
	}
	
	public void setFailState(String emsState) {
		failState = emsState;
	}
	
	public String getFailState() {
		return failState;
	}
	public int getRetries() {
		return retries;
	}
	public void resetRetries() {
		retries =0;
	}
	
}
