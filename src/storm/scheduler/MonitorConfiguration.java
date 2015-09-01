/*******************************************************************************
* Copyright (c) 2013 Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Eclipse Public License v1.0
* which accompanies this distribution, and is available at
* http://www.eclipse.org/legal/epl-v10.html
*
* Contributors:
* Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni
*******************************************************************************/
package storm.scheduler;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class MonitorConfiguration {

	private static MonitorConfiguration instance = null;
	
	private int timeWindowSlotCount;
	private int timeWindowSlotLength;
	
	private Logger logger;
	
	private MonitorConfiguration() {
		logger = Logger.getLogger(MonitorConfiguration.class);
		
		try {
			// load configuration from file
			logger.debug("Loading configuration from file");
			Properties properties = new Properties();
			properties.load(new FileInputStream("db.ini"));
			logger.debug("Configuration loaded");
			
			timeWindowSlotCount = Integer.parseInt(properties.getProperty("time.window.slot.count"));
			timeWindowSlotLength = Integer.parseInt(properties.getProperty("time.window.slot.length"));
		} catch (Exception e) {
			logger.error("Error loading MonitorConfiguration configuration from file", e);
		}
	}
	
	public synchronized static MonitorConfiguration getInstance() {
		if (instance == null)
			instance = new MonitorConfiguration();
		return instance;
	}
	
	/*
	 * @Return the length of monitoring time window, in seconds
	 */
	public int getTimeWindowLength() {
		return timeWindowSlotCount * timeWindowSlotLength;
	}
	
	public int getTimeWindowSlotLength() {
		return timeWindowSlotLength;
	}
	
	public int getTimeWindowSlotCount() {
		return timeWindowSlotCount;
	}
}
