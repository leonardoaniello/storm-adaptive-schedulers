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

import backtype.storm.scheduler.ExecutorDetails;

public class Executor {

	private int beginTask;
	private int endTask;
	private long load;
	private String node;
	private String topologyID;
	
	public Executor() {
		this(-1,-1);
	}
	
	public boolean match(ExecutorDetails executorDetails) {
		return executorDetails != null && executorDetails.getStartTask() == beginTask && executorDetails.getEndTask() == endTask;
	}
	
	public Executor(int beginTask, int endTask) {
		this.beginTask = beginTask;
		this.endTask = endTask;
	}

	public int getBeginTask() {
		return beginTask;
	}

	public int getEndTask() {
		return endTask;
	}

	public boolean includes(int task) {
		return task >= beginTask && task <= endTask;
	}
	
	public void add(int task) {
		if (beginTask == -1 && endTask == -1) {
			beginTask = task;
			endTask = task;
		}
		else if (task < beginTask)
			beginTask = task;
		else if (task > endTask)
			endTask = task;
	}
	
	@Override
	public String toString() {
		return "[" + beginTask + ", " + endTask + "]; load: " + load + " Hz/s" + ((node!=null)?"; node: " + node:"");
	}

	public long getLoad() {
		return load;
	}

	public void setLoad(long load) {
		this.load = load;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + beginTask;
		result = prime * result + endTask;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Executor other = (Executor) obj;
		if (beginTask != other.beginTask)
			return false;
		if (endTask != other.endTask)
			return false;
		return true;
	}

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public String getTopologyID() {
		return topologyID;
	}

	public void setTopologyID(String topologyID) {
		this.topologyID = topologyID;
	}
}
