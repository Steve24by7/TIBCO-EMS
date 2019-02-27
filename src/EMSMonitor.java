package com.nr.expertservices.tibco.ems.monitor;

/*
 * Program to pull metric data from TIBCO EMS
 * New Relic, Inc.  ALL RIGHTS RESERVED
 * Requires TIBCO libraries linked as tibjms.jar, tibjmsadmin.jar, jms-2.0.jar
 * Also the newrelic metrics_publish.2.2.0.jar
 * Created 2015-12-09
 * Modification History:
 * 1.9.2 2018-11-27 getServerInfo
 * 
 */


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tibco.tibjms.admin.*;
import com.newrelic.expertservices.insights.InsightPost;
import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;

public class EMSMonitor extends Agent {

	private static final String GUID = "com.nr.expertservices.tibco.ems.monitor";
	private static final String version = "3.0.2";
	private String name;
	private EMSServer emsServer;
	private static final Logger logger = Logger.getLogger(EMSMonitor.class);
	private InsightPost insights;
	private boolean sendToInsights = false;

	public void setSendToInsights(boolean sendToInsights) {
		this.sendToInsights = sendToInsights;
	}

	public EMSMonitor(EMSServer ems) {
		super(GUID, version);
		emsServer = ems;
		name = "EMS Monitor - " + emsServer.getName();
	}

	@Override
	public String getAgentName() {
		return name;
	}
	
	

	public void createInsightPost(String key, int account) {
		insights = new InsightPost(account, key);
		sendToInsights = true;
	}

	
	@Override
	public void pollCycle() {
		logger.debug("pollCycle: ", emsServer.getName());
		try {
//			TibjmsAdmin tibjmsServer = connect(emsServer);
			if(emsServer == null) {
				logger.error("emsServer ",name," is null");
			}
			TibjmsAdmin tibjmsServer = emsServer.getConnection(emsServer);
			if (tibjmsServer == null) {
				logger.error("tibjmsServer ",name," is null");
				return;
			}
			String serverStatus = tibjmsServer.getStateInfo().getState().toString();
			logger.debug("Server ",name," is ",serverStatus);
			if(serverStatus == "active" ) {
			getServerInfo(tibjmsServer);
			getQueueStats(tibjmsServer);
			getTopicStats(tibjmsServer);
			getBridgeStats(tibjmsServer);
			getRouteStats(tibjmsServer);
			getStoreInfo(tibjmsServer);
//			tibjmsServer.close();
			} else {
				logger.debug("Server ",name," is not active");
			}
		} catch (TibjmsAdminException e) {
			e.printStackTrace();
			logger.info("emsServer ",name," connection retrying.");
			emsServer.setFailState("failed");
			if(emsServer.getRetries() > 5) {
				logger.info("emsServer ",name," reset retries, sleeping 5min.");
				emsServer.resetRetries();
				try {
					Thread.sleep(300000);  /* wait 5 minutes */
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			TibjmsAdmin tibjmsServer = emsServer.getConnection(emsServer);
			}
	}




	protected void getQueueStats(TibjmsAdmin tibjmsServer) {
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting queue metrics on ",emsServerName);
			String prefix = "EMS Queues/"+emsServerName;
			QueueInfo[] queueList = tibjmsServer.getQueues();

			List<String> ignores = emsServer.getQueueIgnores();
			HashMap<String,String> labels = emsServer.getLabel();

			long totalDelivered = 0;
			int totalConsumers = 0;
			int totalReceivers = 0;
			long totalInTransit = 0;
			long totalPendingCount = 0;
			long totalPendingSize = 0;
			long totalPersistentCount = 0;
			long totalPersistendSize = 0;
			int totalPrefetch = 0;
			int queues = 0;

			if (queueList != null) 
				for (QueueInfo q : queueList) {
					boolean skip = false;
					String qName = q.getName();
					if (q.isTemporary() && !emsServer.getFlagIncludeDynamicQueues()) skip = true;
					for(int i=0;i<ignores.size();i++) {
						String ignore = ignores.get(i);
						if(qName.matches(ignore)) {
							skip = true;
							logger.debug("Skipping metrics for ",qName);
							break;
						}

					}

					if (!skip) {
						queues++;
						HashMap<String, Object> insightsMap = new HashMap<String, Object>();
	
						String queueName = qName.trim();
						logger.debug("Reporting metrics for queue: ",queueName);
						
						long deliveredCount = q.getDeliveredMessageCount();
						reportMetric(prefix + "/" + queueName + "/Delivered Count", "messages",  deliveredCount);
						insightsMap.put("Delivered Count", deliveredCount);
						totalDelivered += deliveredCount;
						int consumerCount = q.getConsumerCount();
						reportMetric(prefix + "/" + queueName + "/Consumer Count", "consumers", consumerCount);
						insightsMap.put("Consumer Count", consumerCount);
						totalConsumers += consumerCount;
						int receiverCount = q.getReceiverCount();
						reportMetric(prefix + "/" + queueName + "/Receiver Count", "connections",  receiverCount);
						insightsMap.put("Receiver Count", receiverCount);
						totalReceivers += receiverCount;
						long inTransitCount = q.getInTransitMessageCount();
						reportMetric(prefix + "/" + queueName + "/InTransit Count", "messages",  inTransitCount);
						insightsMap.put("In Transit Count", inTransitCount);
						totalInTransit += inTransitCount;

						reportMetric(prefix + "/" + queueName + "/Configuration/Max Redelivery Count", "tries", q.getMaxRedelivery());
						insightsMap.put("Maximum Redelivery Count", q.getMaxRedelivery());
						reportMetric(prefix + "/" + queueName + "/Configuration/Max Bytes", "bytes", q.getMaxBytes());
						insightsMap.put("Maximum Bytes", q.getMaxBytes());
						reportMetric(prefix + "/" + queueName + "/Configuration/Max Messages", "messages", q.getMaxMsgs());
						insightsMap.put("Maximum Messages", q.getMaxMsgs());
						long pendingMessageCount = q.getPendingMessageCount();
						reportMetric(prefix + "/" + queueName + "/Pending Message Count", "messages",pendingMessageCount);
						insightsMap.put("Pending Messages", pendingMessageCount);
						totalPendingCount += pendingMessageCount;
						long pendingMessageSize = q.getPendingMessageSize();
						reportMetric(prefix + "/" + queueName + "/Pending Message Total", "messages",pendingMessageSize);
						insightsMap.put("Pending Message Total", pendingMessageSize);
						totalPendingSize += pendingMessageSize;
						long persisentMessageCount = q.getPendingPersistentMessageCount();
						reportMetric(prefix + "/" + queueName + "/Pending Persistent Count", "messages", persisentMessageCount);
						insightsMap.put("Pending Persistent Count", persisentMessageCount);
						totalPersistentCount += persisentMessageCount;
						long persistentMessageSize = q.getPendingPersistentMessageSize();
						reportMetric(prefix + "/" + queueName + "/Pending Persistent Total", "messages", persistentMessageSize);
						insightsMap.put("Pending Persistent Size", persistentMessageSize);
						totalPersistendSize += persistentMessageSize;
						int prefetch = q.getPrefetch();
						reportMetric(prefix + "/" + queueName + "/Configuration/Prefetch Total", "messages", prefetch);
						insightsMap.put("Prefetch", prefetch);
						totalPrefetch += prefetch;

						reportMetric(prefix + "/" + queueName + "/Inbound/Byte Rate", "bytes/sec", q.getInboundStatistics().getByteRate());
						insightsMap.put("Inbound Byte Rate", q.getInboundStatistics().getByteRate());
						reportMetric(prefix + "/" + queueName + "/Inbound/Message Rate", "messages/sec", q.getInboundStatistics().getMessageRate());
						insightsMap.put("Inbound Message Rate", q.getInboundStatistics().getMessageRate());
						reportMetric(prefix + "/" + queueName + "/Inbound/Total Bytes", "bytes", q.getInboundStatistics().getTotalBytes());
						insightsMap.put("Inbound Total Bytes", q.getInboundStatistics().getTotalBytes());
						reportMetric(prefix + "/" + queueName + "/Inbound/Total Messages", "messages", q.getInboundStatistics().getTotalMessages());
						insightsMap.put("Inbound Total Messages", q.getInboundStatistics().getTotalMessages());

						reportMetric(prefix + "/" + queueName + "/Outbound/Byte Rate", "bytes/sec", q.getOutboundStatistics().getByteRate());
						insightsMap.put("Outbound Byte Rate", q.getOutboundStatistics().getByteRate());
						reportMetric(prefix + "/" + queueName + "/Outbound/Message Rate", "messages/sec", q.getOutboundStatistics().getMessageRate());
						insightsMap.put("Outbound Message Rate", q.getOutboundStatistics().getMessageRate());
						reportMetric(prefix + "/" + queueName + "/Outbound/Total Bytes", "bytes", q.getOutboundStatistics().getTotalBytes());
						insightsMap.put("Outbound Total Bytes", q.getOutboundStatistics().getTotalBytes());
						reportMetric(prefix + "/" + queueName + "/Outbound/Total Messages", "messages", q.getOutboundStatistics().getTotalMessages());
						insightsMap.put("Outbound Total Messages", q.getOutboundStatistics().getTotalMessages());
						
						if(q.isRouted()) {
							String routeName = q.getRouteName();
							if(routeName != null && !routeName.isEmpty()) {
								insightsMap.put("Route Name", q.getRouteName());
							}
						}
						if(!labels.isEmpty()) {
							Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
							while (it.hasNext()) {
								Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
								insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
							}
						}
						

						if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
							try {
								insightsMap.put("Queue Name", queueName);
								insightsMap.put("Server Name", emsServerName);
								insights.post("EMSQueue", insightsMap);
							} catch (Exception e) {
								logger.error(e, "Failed to send to Insights");
							}
						}
						String statString = q.statString();
						logger.debug("Statistics string delivered: ",statString);						

					}

				}
			//			reportMetric(prefix + "/Critical Capacity", "queues", number_critical);
			//			reportMetric(prefix + "/Danger Capacity", "queues", number_warning);
			if(queues > 0) {
				HashMap<String, Object> insightsMap = new HashMap<String, Object>();
				reportMetric(prefix + "/Total Consumers", "consumers", totalConsumers);
				insightsMap.put("Total Consumers", totalConsumers);
				reportMetric(prefix + "/Total Pending Count", "messages", totalPendingCount);
				insightsMap.put("Total Pending Count", totalPendingCount);
				reportMetric(prefix + "/Total Pending Size", "bytes", totalPendingSize);
				insightsMap.put("Total Pending Size", totalPendingSize);
				reportMetric(prefix + "/Total Pending Persistent Count", "messages", totalPersistentCount);
				insightsMap.put("Total Pending Persisent Count", totalPersistentCount);
				reportMetric(prefix + "/Total Pending Persistent Size", "bytes", totalPersistendSize);
				insightsMap.put("Total Pending Persistent Size", totalPersistendSize);
				reportMetric(prefix + "/Total Prefetch", "messages", totalPrefetch);
				insightsMap.put("Total Prefetch", totalPrefetch);
				reportMetric(prefix + "/Total Delivered", "messages", totalDelivered);
				insightsMap.put("Total Delivered", totalDelivered);
				reportMetric(prefix + "/Total Receivers", "receivers", totalReceivers);
				insightsMap.put("Total Receivers", totalReceivers);
				reportMetric(prefix + "/Total In Transit", "messages", totalInTransit);
				insightsMap.put("Total In Transit", totalInTransit);
				
				if(!labels.isEmpty()) {
					Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
						insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
					}
				}
				if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
					try {
						insightsMap.put("Total Monitored Queues", queues);
						insightsMap.put("Server Name", emsServerName);
						insights.post("EMSQueues", insightsMap);
					} catch (Exception e) {
						logger.error(e, "Failed to send to Insights");
					}
				}
			}

		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

	protected void getTopicStats(TibjmsAdmin tibjmsServer) {
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting queue metrics on ",emsServerName);
			String prefix = "EMS Topics";
			TopicInfo[] topicList = tibjmsServer.getTopics();

			List<String> ignores = emsServer.getTopicIgnores();
			HashMap<String,String> labels = emsServer.getLabel();

			int totalConsumers = 0;
			long totalPendingCount = 0;
			long totalPendingSize = 0;
			long totalPersistentCount = 0;
			long totalPersistendSize = 0;
			int totalPrefetch = 0;
			int totalActiveDurable = 0;
			int totalDurableSubscriptions = 0;
			int totalSubscriberCount = 0;
			int totalSubscriptionCount = 0;
			int topics = 0;

			if (topicList != null)  {
				for (TopicInfo t : topicList) {
					boolean skip = false;
					String tName = t.getName();
					if (t.isTemporary() && !emsServer.getFlagIncludeDynamicTopics()) skip = true;
					for(int i=0;i<ignores.size();i++) {
						String ignore = ignores.get(i);
						if(tName.matches(ignore)) {
							skip = true;
							logger.debug("Skipping metrics for ",tName);
							break;
						}

					}

					if (!skip) {
						topics++;
						HashMap<String, Object> insightsMap = new HashMap<String, Object>();
						String topicName = tName.trim();
						logger.debug("Reporting metrics for topic: ",topicName);

						int consumerCount = t.getConsumerCount();
						reportMetric(prefix + "/" + topicName + "/Consumer Count", "consumers", consumerCount);
						insightsMap.put("Consumer Count", consumerCount);
						totalConsumers += consumerCount;
						reportMetric(prefix + "/" + topicName + "/Configuration/Max Bytes", "bytes", t.getMaxBytes());
						insightsMap.put("Maximum Bytes", t.getMaxBytes());
						reportMetric(prefix + "/" + topicName + "/Configuration/Max Messages", "messages", t.getMaxMsgs());
						insightsMap.put("Maximum Messages", t.getMaxMsgs());
						long pendingMessageCount = t.getPendingMessageCount();
						reportMetric(prefix + "/" + topicName + "/Pending Message Count", "messages",pendingMessageCount);
						insightsMap.put("Pending Messages", pendingMessageCount);
						totalPendingCount += pendingMessageCount;
						long pendingMessageSize = t.getPendingMessageSize();
						reportMetric(prefix + "/" + topicName + "/Pending Message Total", "messages",pendingMessageSize);
						insightsMap.put("Pending Message Total", pendingMessageSize);
						totalPendingSize += pendingMessageSize;
						long persisentMessageCount = t.getPendingPersistentMessageCount();
						reportMetric(prefix + "/" + topicName + "/Pending Persistent Count", "messages", persisentMessageCount);
						insightsMap.put("Pending Persistent Count", persisentMessageCount);
						totalPersistentCount += persisentMessageCount;
						long persistentMessageSize = t.getPendingPersistentMessageSize();
						reportMetric(prefix + "/" + topicName + "/Pending Persistent Total", "messages", persistentMessageSize);
						insightsMap.put("Pending Persistent Size", persistentMessageSize);
						totalPersistendSize += persistentMessageSize;
						int prefetch = t.getPrefetch();
						reportMetric(prefix + "/" + topicName + "/Configuration/Prefetch Total", "messages", prefetch);
						insightsMap.put("Prefetch", prefetch);
						totalPrefetch += prefetch;

						int activeDurableCount = t.getActiveDurableCount();
						reportMetric(prefix + "/" + topicName + "/Active Durable Count", "durables", activeDurableCount);
						insightsMap.put("Active Durable Count", activeDurableCount);
						totalActiveDurable += activeDurableCount;

						int durableSubscriptionCount = t.getDurableSubscriptionCount();
						reportMetric(prefix + "/" + topicName + "/Durable Subscription Count", "subscriptions", durableSubscriptionCount);
						insightsMap.put("Durable Subscription Count", durableSubscriptionCount);
						totalDurableSubscriptions += durableSubscriptionCount;


						int subscriberCount = t.getSubscriberCount();
						reportMetric(prefix + "/" + topicName + "/Subscriber Count", "subscribers", subscriberCount);
						insightsMap.put("Subscriber Count", subscriberCount);
						totalSubscriberCount += subscriberCount;
						int subscriptionCount = t.getSubscriptionCount();
						reportMetric(prefix + "/" + topicName + "/Subscription Count", "subscriptions", subscriptionCount);
						insightsMap.put("Subscription Count", subscriptionCount);
						totalSubscriptionCount += subscriptionCount;
						reportMetric(prefix + "/" + topicName + "/Inbound/Byte Rate", "bytes/sec", t.getInboundStatistics().getByteRate());
						insightsMap.put("Inbound Byte Rate", t.getInboundStatistics().getByteRate());
						reportMetric(prefix + "/" + topicName + "/Inbound/Message Rate", "messages/sec", t.getInboundStatistics().getMessageRate());
						insightsMap.put("Inbound Message Rate", t.getInboundStatistics().getMessageRate());
						reportMetric(prefix + "/" + topicName + "/Inbound/Total Bytes", "bytes", t.getInboundStatistics().getTotalBytes());
						insightsMap.put("Inbound Total Bytes", t.getInboundStatistics().getTotalBytes());
						reportMetric(prefix + "/" + topicName + "/Inbound/Total Messages", "messages", t.getInboundStatistics().getTotalMessages());
						insightsMap.put("Inbound Total Messages", t.getInboundStatistics().getTotalMessages());

						reportMetric(prefix + "/" + topicName + "/Outbound/Byte Rate", "bytes/sec", t.getOutboundStatistics().getByteRate());
						insightsMap.put("Outbound Byte Rate", t.getOutboundStatistics().getByteRate());
						reportMetric(prefix + "/" + topicName + "/Outbound/Message Rate", "messages/sec", t.getOutboundStatistics().getMessageRate());
						insightsMap.put("Outbound Message Rate", t.getOutboundStatistics().getMessageRate());
						reportMetric(prefix + "/" + topicName + "/Outbound/Total Bytes", "bytes", t.getOutboundStatistics().getTotalBytes());
						insightsMap.put("Outbound Total Bytes", t.getOutboundStatistics().getTotalBytes());
						reportMetric(prefix + "/" + topicName + "/Outbound/Total Messages", "messages", t.getOutboundStatistics().getTotalMessages());
						insightsMap.put("Outbound Total Messages", t.getOutboundStatistics().getTotalMessages());

						if(!labels.isEmpty()) {
							Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
							while (it.hasNext()) {
								Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
								insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
							}
						}
						if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
							try {
								insightsMap.put("Topic Name", topicName);
								insightsMap.put("Server Name", emsServerName);
								insights.post("EMSTopic", insightsMap);
							} catch (Exception e) {
								logger.error(e, "Failed to send to Insights");
							}
						}

					}

				}
			}
			if(topics > 0) {
				HashMap<String, Object> insightsMap = new HashMap<String, Object>();
				reportMetric(prefix + "/Total Consumers", "consumers", totalConsumers);
				insightsMap.put("Total Consumers", totalConsumers);
				reportMetric(prefix + "/Total Pending Count", "messages", totalPendingCount);
				insightsMap.put("Total Pending Count", totalPendingCount);
				reportMetric(prefix + "/Total Pending Size", "messages", totalPendingSize);
				insightsMap.put("Total Pending Size", totalPendingSize);
				reportMetric(prefix + "/Total Pending Persistent Count", "messages", totalPersistentCount);
				insightsMap.put("Total Pending Persisent Count", totalPersistentCount);
				reportMetric(prefix + "/Total Pending Persistent Size", "messages", totalPersistendSize);
				insightsMap.put("Total Pending Persistent Size", totalPersistendSize);
				reportMetric(prefix + "/Total Prefetch", "messages", totalPrefetch);
				insightsMap.put("Total Prefetch", totalPrefetch);
				reportMetric(prefix + "/Total Active Durables", "durables", totalActiveDurable);
				insightsMap.put("Total Active Durables", totalActiveDurable);
				reportMetric(prefix + "/Total Durable Subscriptions", "subscriptions", totalDurableSubscriptions);
				insightsMap.put("Total Durable Subscriptions", totalDurableSubscriptions);
				reportMetric(prefix + "/Total Subscribers", "subscribers", totalSubscriberCount);
				insightsMap.put("Total Subscribers", totalSubscriberCount);
				reportMetric(prefix + "/Total Subscriptions", "subscriptions", totalSubscriptionCount);
				insightsMap.put("Total Subcriptions", totalSubscriptionCount);

				if(!labels.isEmpty()) {
					Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
						insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
					}
				}
				if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
					try {
						insightsMap.put("Total Monitored Topics", topics);
						insightsMap.put("Server Name", emsServerName);
						insights.post("EMSTopics", insightsMap);
					} catch (Exception e) {
						logger.error(e, "Failed to send to Insights");
					}
				}
			}


		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

	protected void getBridgeStats(TibjmsAdmin tibjmsServer) {
		String prefix = "EMS Bridges";
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting bridge metrics on ",emsServerName);
			BridgeInfo[] bridgeList = tibjmsServer.getBridges();
			

			if (bridgeList != null) 
				for (BridgeInfo t : bridgeList) {

					logger.debug("Reporting metrics for bridge: ",t.getName().trim());
					reportMetric(prefix + "/" + t.getName().trim() + "/Bridge", "messages",  0);
						
				}

		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

	protected void getRouteStats(TibjmsAdmin tibjmsServer) {
		String prefix = "EMS Routes";
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting route metrics on ",emsServerName);
			RouteInfo[] routeList = tibjmsServer.getRoutes();
			long totalBackLogCount = 0;
			long totalBackLogSize = 0;
			int routes = 0;
			
			HashMap<String,String> labels = emsServer.getLabel();

			if (routeList != null) 
				for (RouteInfo t : routeList) {
					routes++;
					String routeName = t.getName().trim();
					logger.debug("Reporting metrics for route: ",t.getName().trim());
					HashMap<String, Object> insightsMap = new HashMap<String, Object>();
				

					reportMetric(prefix + "/" + routeName + "/Backlog Count", "messages", t.getBacklogCount());
					totalBackLogCount += t.getBacklogCount();
					insightsMap.put("Backlog Count", t.getBacklogCount());
					reportMetric(prefix + "/" + routeName + "/Backlog Size", "bytes", t.getBacklogSize());
					insightsMap.put("Backlog Size", t.getBacklogSize());
					totalBackLogSize += t.getBacklogSize();

					reportMetric(prefix + "/" + routeName + "/Inbound/Byte Rate", "bytes/sec", t.getInboundStatistics().getByteRate());
					insightsMap.put("Inbound Byte Rate", t.getInboundStatistics().getByteRate());
					reportMetric(prefix + "/" + routeName + "/Inbound/Message Rate", "messages/sec", t.getInboundStatistics().getMessageRate());
					insightsMap.put("Inbound Message Rate", t.getInboundStatistics().getMessageRate());
					reportMetric(prefix + "/" + routeName + "/Inbound/Total Bytes", "bytes", t.getInboundStatistics().getTotalBytes());
					insightsMap.put("Inbound Total Bytes", t.getInboundStatistics().getTotalBytes());
					reportMetric(prefix + "/" + routeName + "/Inbound/Total Messages", "messages", t.getInboundStatistics().getTotalMessages());
					insightsMap.put("Inbound Total Messages", t.getInboundStatistics().getTotalMessages());

					reportMetric(prefix + "/" + routeName + "/Outbound/Byte Rate", "bytes/sec", t.getOutboundStatistics().getByteRate());
					insightsMap.put("Outbound Byte Rate", t.getOutboundStatistics().getByteRate());
					reportMetric(prefix + "/" + routeName + "/Outbound/Message Rate", "messages/sec", t.getOutboundStatistics().getMessageRate());
					insightsMap.put("Outbound Message Rate", t.getOutboundStatistics().getMessageRate());
					reportMetric(prefix + "/" + routeName + "/Outbound/Total Bytes", "bytes", t.getOutboundStatistics().getTotalBytes());
					insightsMap.put("Outbound Total Bytes", t.getOutboundStatistics().getTotalBytes());
					reportMetric(prefix + "/" + routeName + "/Outbound/Total Messages", "messages", t.getOutboundStatistics().getTotalMessages());
					insightsMap.put("Outbound Total Messages", t.getOutboundStatistics().getTotalMessages());

					insightsMap.put("isStalled", t.isStalled());
					insightsMap.put("isConnected", t.isConnected());

					if(!labels.isEmpty()) {
						Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
							insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
						}
					}
					if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
						try {
							insightsMap.put("Route Name", routeName);
							insightsMap.put("Server Name", emsServerName);
							insights.post("EMSRoutes", insightsMap);
						} catch (Exception e) {
							logger.error(e, "Failed to send to Insights");
						}
					}

				}
			
			if(routes > 0) {
				HashMap<String, Object> insightsMap = new HashMap<String, Object>();
				insightsMap.put("Total Route Backlog Count", totalBackLogCount);
				insightsMap.put("Total Route Backlog Size", totalBackLogSize);
				insightsMap.put("Total Monitored Routes", routes);

				if(!labels.isEmpty()) {
					Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
						insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
					}
				}
				if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
					try {
						insightsMap.put("Server Name", emsServerName);
						insights.post("EMSRoute", insightsMap);
					} catch (Exception e) {
						logger.error(e, "Failed to send to Insights");
					}
				}
			}

		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

	protected void getServerInfo(TibjmsAdmin tibjmsServer) {
		String prefix = "EMS Server Info";
		HashMap<String, Object> insightsMap = new HashMap<String, Object>();
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting Server Info on ",emsServerName);
			StateInfo stateInfo = tibjmsServer.getStateInfo();
			ServerInfo getInfo = tibjmsServer.getInfo();
			
			HashMap<String,String> labels = emsServer.getLabel();

			if (stateInfo != null) 
				{
					String state = tibjmsServer.getStateInfo().getState().toString();
					logger.debug("Reporting state "+ emsServerName+":", state );
					insightsMap.put("Server State", state);

					int pid = stateInfo.getProcessId();
					logger.debug("Reporting PID "+ emsServerName+":", pid);
					insightsMap.put("PID", pid);
					
					int connCount = getInfo.getConnectionCount();
					logger.debug("Reporting Connection Count "+ emsServerName+":", connCount);
					insightsMap.put("Connection Count", connCount);
					
					long totalMsgMemory = getInfo.getMsgMem();
					logger.debug("Reporting totalMsgMem "+ emsServerName+":", totalMsgMemory);
					insightsMap.put("TotalMsgMemory", totalMsgMemory);
					
					long maxMsgMemory = getInfo.getMaxMsgMemory();
					logger.debug("Reporting maxMsgMem "+ emsServerName+":", maxMsgMemory);
					insightsMap.put("MaxMsgMemory", maxMsgMemory);
					
					if(!labels.isEmpty()) {
						Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
							insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
						}
					}

					if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
						try {
							insightsMap.put("Server Name", emsServerName);
							insightsMap.put("connectState", emsServer.getFailState());
							insights.post("EMSServers", insightsMap);
						} catch (Exception e) {
							logger.error(e, "Failed to send to Insights");
						}
					}
				}

		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

	protected void getStoreInfo(TibjmsAdmin tibjmsServer) {
		String prefix = "EMS Store Info";
		HashMap<String, Object> insightsMap = new HashMap<String, Object>();
		try {

			String emsServerName = emsServer.getName().trim();
			logger.debug("Reporting Store Info on ",emsServerName);
//			StoreInfo storeInfo = tibjmsServer.getStoreInfo(emsServerName);
			
			HashMap<String,String> labels = emsServer.getLabel();


			String[] stores = tibjmsServer.getStores();
			for (String s : stores) {
				StoreInfo storeInfo = tibjmsServer.getStoreInfo(s);

				long freeSpaceBytes = storeInfo.getFreeSpace();
				logger.debug("Reporting Store " + s + " FreeSpace Bytes: ", freeSpaceBytes );
				reportMetric(prefix + "/" + emsServerName + "/Store/" + s, "Bytes",  freeSpaceBytes);
				insightsMap.put("Free Space Bytes", freeSpaceBytes);
				
				long storeMsgBytes = storeInfo.getMsgBytes();
				logger.debug("Reporting Store " + s + " Msg Bytes: ", storeMsgBytes );
				reportMetric(prefix + "/" + emsServerName + "/Store/" + s, "Bytes",  storeMsgBytes);
				insightsMap.put("Store Msg Bytes", storeMsgBytes);
				
				double avgWrite = storeInfo.getAverageWriteTime();
				logger.debug("Reporting Store " + s + " Avg Write Time: ", avgWrite );
				reportMetric(prefix + "/" + emsServerName + "/Store/" + s, "ms",  avgWrite);
				insightsMap.put("Avg Write Time", avgWrite);
				
				if(!labels.isEmpty()) {
					Iterator<Map.Entry<String, String>> it = labels.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry<String,String> thisEntry = (Map.Entry<String,String>) it.next();
						insightsMap.put(thisEntry.getKey().toString(), thisEntry.getValue().toString());
					}
				}

				if(!insightsMap.isEmpty() && sendToInsights && insights != null) {
					try {
						insightsMap.put("Store Name", s);
						insightsMap.put("Server Name", emsServerName);
						insights.post("EMSStores", insightsMap);
					} catch (Exception e) {
						logger.error(e, "Failed to send to Insights");
					}
				}
			}

		} catch (TibjmsAdminException e) {
			logger.error(e, "TibJMSAdminException occurred");
		} 

	}

}
