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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import cpuinfo.CPUInfo;

public class WorkerMonitor {

	private static WorkerMonitor instance = null;
	
	private String topologyId;
	private int workerPort;
	private Logger logger;
	
	/*
	 * threadId -> time series of the load
	 */
	private Map<Long, List<Long>> loadStats;
	
	/*
	 * <sourceTaskId, destinationTaskId> -> time series of the traffic
	 */
	private Map<TaskPair, List<Integer>> trafficStats;
	
	/*
	 * threadId -> list of tasks Id, in the form [begin task, end task] = Executor
	 */
	private Map<Long, Executor> threadToTaskMap;
	
	private List<TaskMonitor> taskMonitorList;
	
	/**
	 * map source task id -> number of tuples sent by the source to the acker
	 */
	// private Map<Integer, Integer> ackerTrafficMap;
	
	private int timeWindowSlotCount;
	private int timeWindowSlotLength;
	
	public synchronized static WorkerMonitor getInstance() {
		if (instance == null)
			instance = new WorkerMonitor();
		return instance;
	}
	
	/*public synchronized void notifyTupleSentToAcker(int sourceTaskId) {
		Integer traffic = ackerTrafficMap.get(sourceTaskId);
		if (traffic == null)
			traffic = 0;
		ackerTrafficMap.put(sourceTaskId, ++traffic);
	}*/
	
	private WorkerMonitor() {
		logger = Logger.getLogger(WorkerMonitor.class);
		loadStats = new HashMap<Long, List<Long>>();
		trafficStats = new HashMap<TaskPair, List<Integer>>();
		threadToTaskMap = new HashMap<Long, Executor>();
		taskMonitorList = new ArrayList<TaskMonitor>();
		// ackerTrafficMap = new HashMap<Integer, Integer>();
		
		timeWindowSlotCount = MonitorConfiguration.getInstance().getTimeWindowSlotCount();
		timeWindowSlotLength = MonitorConfiguration.getInstance().getTimeWindowSlotLength();
		
		try {
			DataManager.getInstance().checkNode(CPUInfo.getInstance().getTotalSpeed());
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);
		}
		
		new WorkerMonitorThread().start();
		logger.info("WorkerMonitor started!!");
	}
	
	/*
	 * made once by each task in its nextTuple() or execute() method
	 */
	public synchronized void registerTask(TaskMonitor taskMonitor) {
		Executor executor = threadToTaskMap.get(taskMonitor.getThreadId());
		if (executor == null) {
			executor = new Executor();
			threadToTaskMap.put(taskMonitor.getThreadId(), executor);
		}
		if (!executor.includes(taskMonitor.getTaskId()))
			executor.add(taskMonitor.getTaskId());
		taskMonitorList.add(taskMonitor);
	}
	
	public synchronized void sampleStats() {
		// traffic
		for (TaskMonitor taskMonitor : taskMonitorList) {
			Map<Integer, Integer> taskTrafficStats = taskMonitor.getTrafficStatMap();
			if (taskTrafficStats != null) {
				for (int sourceTaskId : taskTrafficStats.keySet()) {
					notifyTrafficStat(
						new TaskPair(sourceTaskId, taskMonitor.getTaskId()), 
						taskTrafficStats.get(sourceTaskId));
				}
			}
		}
		
		// traffic to the acker
		/*Map<Integer, Integer> tmp = ackerTrafficMap;
		ackerTrafficMap = new HashMap<Integer, Integer>();
		for (int sourceTaskId : tmp.keySet())
			notifyTrafficStat(
				new TaskPair(sourceTaskId, Utils.ACKER_TAKS_ID), 
				tmp.get(sourceTaskId));*/
		
		// load
		Map<Long, Long> loadInfo = LoadMonitor.getInstance().getLoadInfo(threadToTaskMap.keySet());
		for (long threadId : loadInfo.keySet())
			notifyLoadStat(threadId, loadInfo.get(threadId));
	}
	
	/**
	 * 
	 * @param pair
	 * @return average tuples per second sent by pair.source to pair.destination
	 */
	private int getTraffic(TaskPair pair) {
		int total = 0;
		List<Integer> trafficData = trafficStats.get(pair);
		for (int traffic : trafficData)
			total += traffic;
		return (int)((float)total / (trafficData.size() * timeWindowSlotLength));
	}
	
	/**
	 * @param threadID
	 * @return average CPU cycles per second consumed by threadID
	 */
	private long getLoad(long threadID) {
		long total = 0;
		List<Long> loadData = loadStats.get(threadID);
		for (long load : loadData)
			total += load;
		return total / (loadData.size() * timeWindowSlotLength);
	}
	
	public void storeStats() throws Exception {
		
		logger.debug("WorkerMonitor Snapshot");
		logger.debug("----------------------------------------");
		logger.debug("Topology ID: " + topologyId);
		logger.debug("Worker Port: " + workerPort);
		
		logger.debug("Threads to Tasks association:");
		for (long threadId : threadToTaskMap.keySet())
			logger.debug("- " + threadId + ": " + threadToTaskMap.get(threadId));
		
		logger.debug("Inter-Task Traffic Stats (tuples sent per time slot):");
		for (TaskPair pair : trafficStats.keySet()) {
			logger.debug("- " + pair.getSourceTaskId() + "->" + pair.getDestinationTaskId() + ": " + getTraffic(pair) + " tuple/s [" + Utils.collectionToString(trafficStats.get(pair)) + "]");
			DataManager.getInstance().storeTraffic(topologyId, pair.getSourceTaskId(), pair.getDestinationTaskId(), getTraffic(pair));
		}
		
		logger.debug("Load Stats (CPU cycles consumed per time slot):");
		long totalCPUCyclesPerSecond = 0;
		for (long threadId : loadStats.keySet()) {
			List<Long> threadLoadInfo = loadStats.get(threadId);
			totalCPUCyclesPerSecond += threadLoadInfo.get(threadLoadInfo.size() - 1) / timeWindowSlotLength;
			logger.debug("- thread " + threadId + ": " + getLoad(threadId) + " cycle/s [" + Utils.collectionToString(threadLoadInfo) + "]");
			Executor executor = threadToTaskMap.get(threadId);
			DataManager.getInstance().storeLoad(topologyId, executor.getBeginTask(), executor.getEndTask(), getLoad(threadId));
		}
		long totalCPUCyclesAvailable = CPUInfo.getInstance().getTotalSpeed();
		int usage = (int)(((double)totalCPUCyclesPerSecond / totalCPUCyclesAvailable) * 100);
		logger.debug("Total CPU cycles consumed per second: " + totalCPUCyclesPerSecond + ", Total available: " + totalCPUCyclesAvailable + ", Usage: " + usage + "%");
		
		logger.debug("----------------------------------------");
	}
	
	private void notifyLoadStat(long threadId, long load) {
		List<Long> loadList = loadStats.get(threadId);
		if (loadList == null) {
			loadList = new ArrayList<Long>();
			loadStats.put(threadId, loadList);
		}
		loadList.add(load);
		if (loadList.size() > timeWindowSlotCount)
			loadList.remove(0);
	}
	
	private void notifyTrafficStat(TaskPair taskPair, int traffic) {
		List<Integer> trafficList = trafficStats.get(taskPair);
		if (trafficList == null) {
			trafficList = new ArrayList<Integer>();
			trafficStats.put(taskPair, trafficList);
		}
		trafficList.add(traffic);
		if (trafficList.size() > timeWindowSlotCount)
			trafficList.remove(0);
	}

	public String getTopologyId() {
		return topologyId;
	}

	public void setContextInfo(TopologyContext context) {
		this.topologyId = context.getStormId();
		this.workerPort = context.getThisWorkerPort();
		try {
			DataManager.getInstance().checkTopology(topologyId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int getWorkerPort() {
		return workerPort;
	}

}
