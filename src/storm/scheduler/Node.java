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

public class Node {

	private final String name;
	private final long capacity;
	private final int cores;
	private long load;
	private int totalSlotCount;
	private int availableSlotCount;
	private Map<Integer, Slot> slotMap; // slot.hashcode() -> slot
	private int nodeCount;
	
	public Node(String name, long capacity, int cores) {
		this.name = name;
		this.capacity = capacity;
		this.cores = cores;
		slotMap = new HashMap<Integer, Slot>();
	}
	
	public void setNodeCount(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	public long getLoad() {
		return load;
	}
	
	public void addLoad(long load) {
		this.load += load;
	}
	
	public boolean contains(Slot slot) {
		return slotMap.get(slot.hashCode()) != null;
	}
	
	public boolean canAssign(Slot slot) {
		return !contains(slot) && hasAvailableSlots() && canSustainLoad(slot.getLoad()) &&
				getTopologySlotCount(slot.getTopology().getTopologyID()) < slot.getTopology().getMaxNumberOfSlotsPerNode(nodeCount);
	}
	
	public List<Slot> getSlotList() {
		return new ArrayList<Slot>(slotMap.values());
	}
	
	public void assign(Slot slot) {
		if (contains(slot))
			throw new RuntimeException("Slot " + slot + " already assigned to this node");
		if (!hasAvailableSlots())
			throw new RuntimeException("No more slots available for this node");
		if (!canSustainLoad(slot.getLoad()))
			throw new RuntimeException("This node cannot sustain the load of slot " + slot);
		slotMap.put(slot.hashCode(), slot);
		this.load += load;
		availableSlotCount--;
		TrafficManager.getInstance().slotAssigned(this, slot);
	}
	
	public void remove(Slot slot) {
		if (!contains(slot))
			throw new RuntimeException("Cannot remove slot " + slot + " from this node it is not contained here");
		slotMap.remove(slot.hashCode());
		this.load -= load;
		availableSlotCount++;
		TrafficManager.getInstance().slotRemoved(this, slot);
	}

	public int getTotalSlots() {
		return totalSlotCount;
	}

	public void setTotalSlots(int totalSlots) {
		this.totalSlotCount = totalSlots;
		this.availableSlotCount = totalSlots;
	}
	
	public boolean hasAvailableSlots() {
		return availableSlotCount > 0;
	}
	
	public int getAvailableSlotCount() {
		return availableSlotCount;
	}
	
	public boolean canSustainLoad(long load) {
		return this.load + load <= capacity;
	}
	
	public String getName() {
		return name;
	}

	public long getCapacity() {
		return capacity;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		Node other = (Node) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return name + " [cores: " + cores + ", current load: " + load + "/" + capacity + "; available slots: " + availableSlotCount + "/" + totalSlotCount + "]";
	}

	public int getCores() {
		return cores;
	}
	
	public int getTopologySlotCount(String topologyId) {
		int n = 0;
		for (Slot slot : slotMap.values())
			if (slot.getTopology().getTopologyID().equals(topologyId))
				n++;
		return n;
		// return slotMap.keySet().size();
	}
}
