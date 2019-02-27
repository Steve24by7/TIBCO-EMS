package com.nr.expertservices.tibco.ems.monitor;

import com.newrelic.metrics.publish.Runner;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

public class Main {

	public static void main(String[] args) {
		try {
			Runner runner = new Runner();
			runner.add(new EMSMonitorFactory());
			runner.setupAndRun();
		} catch (ConfigurationException e) {
			System.err.println("ERROR: "+e.getMessage());
			System.exit(-1);
		}
		
	}

}
