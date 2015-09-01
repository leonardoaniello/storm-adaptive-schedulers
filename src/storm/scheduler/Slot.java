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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Slot {

	private final Topology topology;
	private final int slotID;
	private Map<Integer, Executor> executorMap; // executor.hashcode() -> executor
	private long load;
	
	public Slot(Topology topology, int slotID) {
		this.topology = topology;
		this.slotID = slotID;
		executorMap = new HashMap<Integer, Executor>();
	}
	
	/**
	 * @return true if this slot can be assigned additional executors
	 */
	public boolean canAccept(Executor executor) {
		return canAccept(1, executor.getLoad());
	}
	
	public boolean canAccept(int executorCount, long totalLoad) {
		return
			topology.getMaxExecutorsPerSlot() - executorMap.values().size() >= executorCount &&
			(topology.getMaxLoadForASlot() < 0 || load + totalLoad <= topology.getMaxLoadForASlot());
		// return topology.getMaxExecutorsPerSlot() - executorMap.values().size() >= executorCount;
	}
	
	@Override
	public String toString() {
		return "{" + topology.getTopologyID() + ", slot ID " + slotID + " (load: " + load + " Hz/s): " + Utils.collectionToString(executorMap.values()) + "}";
	}
	
	/**
	 * assigns the executor to this slot and update inter-slot traffic stats
	 * @param executor
	 */
	public void assign(Executor executor) {
		if (contains(executor))
			throw new RuntimeException("Executor " + executor + " already assigned to slot " + this);
		if (!canAccept(executor))
			throw new RuntimeException("Executor " + executor + " cannot be added to slot " + this);
		executorMap.put(executor.hashCode(), executor);
		load += executor.getLoad();
		TrafficManager.getInstance().executorAssigned(this, executor);
	}
	
	/**
	 * removes the executor from this slot and update inter-slot traffic stats
	 * @param executor
	 */
	public void remove(Executor executor) {
		if (contains(executor)) {
			executorMap.remove(executor.hashCode());
			load -= executor.getLoad();
			TrafficManager.getInstance().executorRemoved(this, executor);
		} else {
			throw new RuntimeException("Executor " + executor + " is not contained in this slot: " + this);
		}
	}
	
	public boolean contains(Executor executor) {
		return executorMap.containsKey(executor.hashCode());
	}
	
	public long getLoad() {
		return load;
	}
	
	public Collection<Executor> getExecutors() {
		return executorMap.values();
	}

	public Topology getTopology() {
		return topology;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + slotID;
		result = prime * result
				+ ((topology == null) ? 0 : topology.hashCode());
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
		Slot other = (Slot) obj;
		if (slotID != other.slotID)
			return false;
		if (topology == null) {
			if (other.topology != null)
				return false;
		} else if (!topology.equals(other.topology))
			return false;
		return true;
	}
}
