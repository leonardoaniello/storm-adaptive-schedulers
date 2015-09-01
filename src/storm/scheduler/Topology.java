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
import java.util.List;

import backtype.storm.scheduler.TopologyDetails;

/**
 * models the slots for a topology
 * @author Leonardo
 *
 */
public class Topology {

	/**
	 * the list of slots for this topology
	 */
	private List<Slot> slotList;
	
	private final String topologyID;
	
	private int executorCount;
	
	private final float alfa, beta, gamma, delta;
	private long totalLoad;
	private int maxExecutorsPerSlot;
	
	@Override
	public String toString() {
		return topologyID + "[slot count: " + slotList.size() + ", alfa: " + alfa + ", beta: " + beta + "]";
	}
	
	public Topology(TopologyDetails details) {
		this.topologyID = details.getId();
		executorCount = details.getExecutors().size();
		int slotCount = Math.min(details.getNumWorkers(), executorCount);
		slotList = new ArrayList<Slot>();
		for (int i = 0; i < slotCount; i++)
			slotList.add(new Slot(this, i));
		
		if (details.getConf().get(Utils.ALFA) != null)
			alfa = Float.parseFloat((String)details.getConf().get(Utils.ALFA));
		else
			alfa = 0;
		
		if (details.getConf().get(Utils.BETA) != null)
			beta = Float.parseFloat((String)details.getConf().get(Utils.BETA));
		else
			beta = 1;
		
		if (details.getConf().get(Utils.GAMMA) != null)
			gamma = Float.parseFloat((String)details.getConf().get(Utils.GAMMA));
		else
			gamma = -1; // a negative value means that no control on the load is enforced at all
		
		if (details.getConf().get(Utils.DELTA) != null)
			delta = Float.parseFloat((String)details.getConf().get(Utils.DELTA));
		else
			delta = 0;
		
		if (alfa < 0 || alfa > 1)
			throw new RuntimeException("Wrong alfa value: " + alfa);
		if (beta < 0 || beta > 1)
			throw new RuntimeException("Wrong beta value: " + beta);
		if (gamma < 1)
			throw new RuntimeException("Wrong gamma value: " + gamma);
		if (delta < 0 || delta > 1)
			throw new RuntimeException("Wrong delta value: " + delta);
		
		int min = (int)Math.ceil((double)executorCount/slotList.size());
		int max = executorCount - slotList.size() + 1;
		maxExecutorsPerSlot = min + (int)Math.ceil(alfa * (max - min));
	}
	
	/**
	 * @param nodeCount
	 * @return the number of nodes to use for this topology, given the total number of nodes in the cluster (controlled by beta parameter)
	 */
	public int getNumberOfNodesToUse(int nodeCount) {
		nodeCount = Math.min(nodeCount, slotList.size());
		int min = (int)Math.ceil((double)slotList.size() / nodeCount);
		int max = Math.min(nodeCount, slotList.size());
		return min + (int)Math.ceil(beta * (max - min));
	}
	
	public int getMaxNumberOfSlotsPerNode(int totalNodeCount) {
		int nodeCount = getNumberOfNodesToUse(totalNodeCount);
		int slotCount = slotList.size();
		int min = (int)Math.ceil((double)slotCount / nodeCount);
		int max = slotCount - nodeCount + 1;
		return min + (int)Math.ceil((double)delta * (max - min));
	}
	
	public long getMaxLoadForASlot() {
		return (long)(gamma * (totalLoad / slotList.size()));
	}
	
	/**
	 * @return the list of slots that have no executors assigned to
	 */
	public List<Slot> getEmptySlots() {
		List<Slot> emptySlotList = new ArrayList<Slot>();
		for (Slot slot : slotList) {
			if (slot.getExecutors().isEmpty())
				emptySlotList.add(slot);
		}
		return emptySlotList;
	}
	
	public List<Slot> getUsedSlots() {
		List<Slot> usedSlotList = new ArrayList<Slot>();
		for (Slot slot : slotList)
			if (!slot.getExecutors().isEmpty())
				usedSlotList.add(slot);
		return usedSlotList;
	}
	
	/**
	 * @return the maximum number of executors per slot
	 */
	public int getMaxExecutorsPerSlot() {
		return maxExecutorsPerSlot;
	}
		
	/**
	 * @param executor
	 * @return the least loaded slot that can accept the given executor
	 */
	public Slot getLeastLoadedSlot(Executor executor) {
		return getLeastLoadedSlot(1, executor.getLoad());
	}
	
	/**
	 * @param e1
	 * @param e2
	 * @return the least loaded slot that can accept both the given executors
	 */
	public Slot getLeastLoadedSlot(Executor e1, Executor e2) {
		return getLeastLoadedSlot(2, e1.getLoad() + e2.getLoad());
	}

	/**
	 * @param executorPair
	 * @return the least loaded node that can accept the more-loaded executor of the given pair
	 */
	public Slot getLeastLoadedSlot(ExecutorPair executorPair) {
		long load = Math.max(executorPair.getSource().getLoad(), executorPair.getDestination().getLoad());
		return getLeastLoadedSlot(1, load);
	}

	private Slot getLeastLoadedSlot(int executorCount, long load) {
		Slot leastLoaded = null;
		for (Slot slot : slotList)
			if ((leastLoaded == null || slot.getLoad() < leastLoaded.getLoad()) && slot.canAccept(executorCount, load))
				leastLoaded = slot;
		return leastLoaded;
	}
	
	/**
	 * @param executor
	 * @return the slot that contains e
	 */
	public Slot getSlot(Executor executor) {
		for (Slot slot : slotList)
			if (slot.contains(executor))
				return slot;
		return null;
	}

	/**
	 * @param e1
	 * @param e2
	 * @return the list of slots that contain e1 and e2 (can be 0, 1 or 2 slots)
	 */
	public List<Slot> getContainingSlotList(Executor e1, Executor e2) {
		List<Slot> sl = new ArrayList<Slot>();
		for (Slot s : slotList)
			if (s.contains(e1) || s.contains(e2))
				sl.add(s);
		
		// sanity check
		if (sl.size() > 2)
			throw new RuntimeException("Found more than 2 slots where executors " + e1 + " and " + e2 + " are assigned!!");
		
		return sl;
	}
	
	public List<Slot> getSlots() {
		return slotList;
	}

	public String getTopologyID() {
		return topologyID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((topologyID == null) ? 0 : topologyID.hashCode());
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
		Topology other = (Topology) obj;
		if (topologyID == null) {
			if (other.topologyID != null)
				return false;
		} else if (!topologyID.equals(other.topologyID))
			return false;
		return true;
	}

	public float getAlfa() {
		return alfa;
	}

	public float getBeta() {
		return beta;
	}
	
	public float getGamma() {
		return gamma;
	}

	public float getDelta() {
		return delta;
	}

	public long getTotalLoad() {
		return totalLoad;
	}

	public void setTotalLoad(long totalLoad) {
		this.totalLoad = totalLoad;
	}
}
