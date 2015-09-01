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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import cpuinfo.CPUInfo;

public class LoadMonitor {
	
	private static final int SECS_TO_NANOSECS = 1000000000;

	private static LoadMonitor instance = null;
	
	private final long cpuSpeed; // Hz
	
	Map<Long, Long> loadHistory;
	
	public static LoadMonitor getInstance() {
		if (instance == null)
			instance = new LoadMonitor();
		return instance;
	}
	
	private LoadMonitor() {
		cpuSpeed = CPUInfo.getInstance().getCoreInfo(0).getSpeed();
	}
	
	public Map<Long, Long> getLoadInfo(Set<Long> threadIds) {
		// get current load
		Map<Long, Long> currentLoadInfo = new HashMap<Long, Long>();
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
		for (long id : threadIds)
			currentLoadInfo.put(id, threadBean.getThreadCpuTime(id));
		
		// compute difference wrt history
		Map<Long, Long> loadInfo = new HashMap<Long, Long>();
		for (long id : threadIds) {
			// Long oldObj = (loadHistory != null)?loadHistory.get(id):0;
			// long old = (oldObj != null)?oldObj.longValue():0;
			long old = 0;
			if (loadHistory != null && loadHistory.get(id) != null)
				old = loadHistory.get(id);
			double deltaTime = (double)(currentLoadInfo.get(id) - old) / SECS_TO_NANOSECS; // sec
			loadInfo.put(id, (long)(deltaTime * cpuSpeed));
		}
		
		// replace history with current
		loadHistory = currentLoadInfo;
		
		return loadInfo;
	}
}
