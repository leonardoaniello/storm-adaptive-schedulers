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

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public class TaskMonitor {
	
	private final int taskId;
	
	private long threadId;
	
	/**
	 * in ms
	 */
	int slotLength;
	
	long lastCheck;
	
	/**
	 * map source task id -> number of tuples sent by source to to this task
	 */
	private Map<Integer, Integer> trafficStatMap;
	
	private Map<Integer, Integer> trafficStatToReturn;
	
	public TaskMonitor(int taskId) {
		this.taskId = taskId;
		threadId = -1;
		slotLength = MonitorConfiguration.getInstance().getTimeWindowSlotLength() * 1000;
		trafficStatMap = new HashMap<Integer, Integer>();
	}
	
	public void checkThreadId() {
		if (threadId == -1) {
			threadId = Thread.currentThread().getId();
			WorkerMonitor.getInstance().registerTask(this);
		}
	}

	public int getTaskId() {
		return taskId;
	}

	public long getThreadId() {
		return threadId;
	}
	
	public void notifyTupleReceived(Tuple tuple) {
		checkThreadId();
		int sourceTaskId = tuple.getSourceTask();
		Integer traffic = trafficStatMap.get(sourceTaskId);
		if (traffic == null)
			traffic = 0;
		trafficStatMap.put(sourceTaskId, ++traffic);
		
		long now = System.currentTimeMillis();
		if (lastCheck == 0)
			lastCheck = now;
		if (now - lastCheck >= slotLength) {
			synchronized (this) {
				trafficStatToReturn = trafficStatMap;
				trafficStatMap = new HashMap<Integer, Integer>();
				lastCheck += slotLength;
			}
		}
	}
	
	/*public void notifySpoutTupleEmitted() {
		WorkerMonitor.getInstance().notifyTupleSentToAcker(taskId);
	}
	
	public void notifyAckSent() {
		WorkerMonitor.getInstance().notifyTupleSentToAcker(taskId);
	}
	
	public void notifyAckFailReceived() {
		checkThreadId();
		Integer traffic = trafficStatMap.get(Utils.ACKER_TAKS_ID);
		if (traffic == null)
			traffic = 0;
		trafficStatMap.put(Utils.ACKER_TAKS_ID, ++traffic);
	}*/
	
	/*
	 * source task -> number of tuples sent to this task
	 */
	public synchronized Map<Integer, Integer> getTrafficStatMap() {
		/*Map<Integer, Integer> tmp = trafficStatMap;
		trafficStatMap = new HashMap<Integer, Integer>();
		return tmp;*/
		return trafficStatToReturn;
	}

}
