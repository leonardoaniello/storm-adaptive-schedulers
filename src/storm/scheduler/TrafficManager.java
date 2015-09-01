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

public class TrafficManager {
	
	private static TrafficManager instance = null;
	
	/**
	 * map topologyID -> list of executor pairs, sorted by traffic descending
	 */
	// private Map<String, List<ExecutorPair>> interExecutorTrafficMap;
	
	/**
	 * for each executor, the list of the executors it communicates with is kept, together with the stats about inter-executor traffic
	 * 
	 * topologyID -> (executor -> list of executors it communicates with)
	 */
	private Map<String, Map<Executor, List<ExecutorPair>>> compiledInterExecutorTrafficMap;
	
	/**
	 * map topologyID -> list of slot pair, sorted by traffic descending
	 */
	private Map<String, List<SlotPair>> interSlotTrafficMap;
	
	/**
	 * map topologyID -> (slot -> list of slots it communicates with)
	 */
	private Map<String, Map<Slot, List<SlotPair>>> compiledInterSlotTrafficMap;
	
	/**
	 * list of node pairs, sorted by traffic descending
	 */
	private List<NodePair> interNodeTrafficList;
	
	/**
	 * slot -> the node it is assigned to
	 */
	private Map<Slot, Node> slotToNodeMap;
	
	/**
	 * node -> list of slots assigned to that node
	 */
	private Map<Node, List<Slot>> nodeToSlotsMap;
	
	/**
	 * topology -> list of nodes where topology's slots are assigned
	 */
	private Map<Topology, List<Node>> topologyToNodesMap;

	
	public static TrafficManager getInstance() {
		if (instance == null)
			instance = new TrafficManager();
		return instance;
	}
	
	public void clear() {
		// interExecutorTrafficMap.clear();
		compiledInterExecutorTrafficMap.clear();
		interSlotTrafficMap.clear();
		compiledInterSlotTrafficMap.clear();
		interNodeTrafficList.clear();
		slotToNodeMap.clear();
		nodeToSlotsMap.clear();
		topologyToNodesMap.clear();
	}
	
	private TrafficManager() {
		// interExecutorTrafficMap = new HashMap<String, List<ExecutorPair>>();
		compiledInterExecutorTrafficMap = new HashMap<String, Map<Executor,List<ExecutorPair>>>();
		interSlotTrafficMap = new HashMap<String, List<SlotPair>>();
		compiledInterSlotTrafficMap = new HashMap<String, Map<Slot,List<SlotPair>>>();
		interNodeTrafficList = new ArrayList<NodePair>();
		slotToNodeMap = new HashMap<Slot, Node>();
		nodeToSlotsMap = new HashMap<Node, List<Slot>>();
		topologyToNodesMap = new HashMap<Topology, List<Node>>();
	}
	
	/**
	 * @param topologyID
	 * @return the list of communicating executor pairs of the given topology, sorted by traffic descending
	 * @throws Exception
	 */
	public List<ExecutorPair> getInterExecutorTrafficList(String topologyID) throws Exception {
		/*List<ExecutorPair> interExecutorTrafficList = interExecutorTrafficMap.get(topologyID);
		if (interExecutorTrafficList == null || interExecutorTrafficList.isEmpty()) {
			interExecutorTrafficList = DataManager.getInstance().getInterExecutorTrafficList(topologyID);
			interExecutorTrafficMap.put(topologyID, interExecutorTrafficList);
			compileInterExecutorTrafficStat(topologyID, interExecutorTrafficList);
		}*/
		
		List<ExecutorPair> interExecutorTrafficList = DataManager.getInstance().getInterExecutorTrafficList(topologyID);
		compileInterExecutorTrafficStat(topologyID, interExecutorTrafficList);
		return interExecutorTrafficList;
	}
	
	/**
	 * Given the list of executor pairs, creates a map executor -> list of executors it communicates with
	 * and puts it in compiledInterExecutorTrafficMap
	 * @param topologyID
	 * @param executorPairList
	 */
	private void compileInterExecutorTrafficStat(String topologyID, List<ExecutorPair> executorPairList) {
		Map<Executor, List<ExecutorPair>> trafficMap = new HashMap<Executor, List<ExecutorPair>>();
		for (ExecutorPair pair : executorPairList) {
			getExecutorPairs(trafficMap, pair.getSource()).add(pair);
			getExecutorPairs(trafficMap, pair.getDestination()).add(pair);
		}
		compiledInterExecutorTrafficMap.put(topologyID, trafficMap);
	}
	
	/**
	 * useful method to create an instance of List<ExecutorPair> in case it doesn't exist yet in trafficMap,
	 * used only by compileTrafficStat() method
	 * @param trafficMap
	 * @param executor
	 * @return
	 */
	private List<ExecutorPair> getExecutorPairs(Map<Executor, List<ExecutorPair>> trafficMap, Executor executor) {
		List<ExecutorPair> executorPairList = trafficMap.get(executor);
		if (executorPairList == null) {
			executorPairList = new ArrayList<ExecutorPair>();
			trafficMap.put(executor, executorPairList);
		}
		return executorPairList;
	}
	
	/**
	 * Searches for the pair identified by given slots.
	 * The order these slots are provided is not relevant, both the combination are checked.
	 * Indeed, while inter-executor traffic has a precise direction (source-> destination), inter-slot traffic hasn't and this stas includes streams in both directions
	 * If the required slot pair doesn't exist yet, it gets created.
	 * @param s1
	 * @param s2
	 * @return
	 */
	private SlotPair getSlotPair(Slot s1, Slot s2) {
		SlotPair slotPair = null;
		List<SlotPair> slotPairList = getSlotPairList(s1.getTopology().getTopologyID());
		for (SlotPair sp : slotPairList)
			if ((sp.getFirst().equals(s1) && sp.getSecond().equals(s2)) ||
				(sp.getFirst().equals(s2) && sp.getSecond().equals(s1)) ) {
				slotPair = sp;
				break;
			}
		
		if (slotPair == null) {
			slotPair = new SlotPair(s1, s2);
			slotPairList.add(slotPair);
		}
		return slotPair;
	}
	
	/**
	 * @param topologyID
	 * @return the slot pairs list for the given topology; it gets created in case it doesn't exist yet
	 */
	private List<SlotPair> getSlotPairList(String topologyID) {
		List<SlotPair> slotPairList = interSlotTrafficMap.get(topologyID);
		if (slotPairList == null) {
			slotPairList = new ArrayList<SlotPair>();
			interSlotTrafficMap.put(topologyID, slotPairList);
		}
		return slotPairList;
	}
	
	/**
	 * Invoked when the given executor is assigned to the specified slot.
	 * Inter-slot traffic stats are updated accordingly.
	 * @param slot
	 * @param executor
	 */
	public void executorAssigned(Slot slot, Executor executor) {
		// get all the executors communicating with input executor
		List<ExecutorPair> executorPairList = compiledInterExecutorTrafficMap.get(executor.getTopologyID()).get(executor);
		for (ExecutorPair executorPair : executorPairList) {
			// for each of them, identify the slot it is currently assigned to, if any
			Executor otherExecutor = executorPair.getSource();
			if (otherExecutor.equals(executor))
				otherExecutor = executorPair.getDestination();
			Slot s = slot.getTopology().getSlot(otherExecutor);
			
			// if it is already assigned to a slot different from the input one, update inter-slot traffic stats
			if (s != null && !s.equals(slot)) {
				// get the proper slot pair (or create if it doesn't exist yet)
				SlotPair slotPair = getSlotPair(slot, s);
				
				// add the traffic and keep the list sorted
				slotPair.addTraffic(executorPair.getTraffic());
				List<SlotPair> slotPairList = getSlotPairList(slot.getTopology().getTopologyID());
				int index = slotPairList.indexOf(slotPair);
				while (index > 0 && slotPair.getTraffic() > slotPairList.get(index - 1).getTraffic()) {
					SlotPair tmp = slotPairList.remove(index - 1);
					slotPairList.add(index, tmp);
					index--;
				}
			}
		}
		Logger.getLogger(TrafficManager.class).debug(
			"After the assignment of executor " + executor + " to slot " + slot + 
			", the inter-slot traffic has become " + Utils.collectionToString(getSlotPairList(slot.getTopology().getTopologyID()))
		);
	}
	
	/**
	 * Invoked when the given executor is removed from the specified slot.
	 * Inter-slot traffic stats are updated accordingly.
	 * @param slot
	 * @param executor
	 */
	public void executorRemoved(Slot slot, Executor executor) {
		// get all the executors communicating with input executor
		List<ExecutorPair> executorPairList = compiledInterExecutorTrafficMap.get(executor.getTopologyID()).get(executor);
		for (ExecutorPair executorPair : executorPairList) {
			// for each of them, identify the slot it is currently assigned to
			Executor otherExecutor = executorPair.getSource();
			if (otherExecutor.equals(executor))
				otherExecutor = executorPair.getDestination();
			Slot s = slot.getTopology().getSlot(otherExecutor);
			
			// sanity check
			/*
			 * this sanity check is wrong because some of the executors might be unassigned yet!!
			 */
			/*if (ss == null)
				throw new RuntimeException("Cannot find the slot where executor " + otherExecutor + " should be assigned");*/
			
			// if it is assigned to a slot different from the input one, update inter-slot traffic stats
			if (s != null && !s.equals(slot)) {
				// get the proper slot pair
				SlotPair slotPair = getSlotPair(slot, s);
				
				// remove the traffic and keep the list sorted
				slotPair.removeTraffic(executorPair.getTraffic());
				List<SlotPair> slotPairList = getSlotPairList(slot.getTopology().getTopologyID());
				int index = slotPairList.indexOf(slotPair);
				while (index < slotPairList.size() - 1 && slotPair.getTraffic() < slotPairList.get(index + 1).getTraffic()) {
					SlotPair tmp = slotPairList.remove(index + 1);
					slotPairList.add(index, tmp);
					index++;
				}
			}
		}
	}
	
	/**
	 * fill in compiledInterSlotTrafficMap data structure (topologyID -> (slot -> list of slot pairs))
	 */
	public void compileInterSlotTraffic() {
		for (String topologyID : interSlotTrafficMap.keySet())
			for (SlotPair slotSetPair : interSlotTrafficMap.get(topologyID)) {
				getSlotPairs(slotSetPair.getFirst()).add(slotSetPair);
				getSlotPairs(slotSetPair.getSecond()).add(slotSetPair);
			}
	}
	
	/**
	 * useful helper method to fill in compiledInterSlotTrafficMap; only called by compileInterSlotTraffic()
	 * @param slot
	 * @return the list of slot pairs communicating with the given slot
	 */
	private List<SlotPair> getSlotPairs(Slot slot) {
		Map<Slot, List<SlotPair>> slotMap = compiledInterSlotTrafficMap.get(slot.getTopology().getTopologyID());
		if (slotMap == null) {
			slotMap = new HashMap<Slot, List<SlotPair>>();
			compiledInterSlotTrafficMap.put(slot.getTopology().getTopologyID(), slotMap);
		}
		List<SlotPair> slotPairList = slotMap.get(slot);
		if (slotPairList == null) {
			slotPairList = new ArrayList<SlotPair>();
			slotMap.put(slot, slotPairList);
		}
		return slotPairList;
	}
	
	/**
	 * @param slot
	 * @return the node where the given slot is assigned, null if the slot is not assigned yet
	 */
	private Node getNode(Slot slot) {
		return slotToNodeMap.get(slot);
	}

	
	/**
	 * invoked when a slot is assigned to a node; updates the inter-node traffic stats
	 * @param node
	 * @param slot
	 */
	public void slotAssigned(Node node, Slot slot) {
		List<SlotPair> slotPairList = compiledInterSlotTrafficMap.get(slot.getTopology().getTopologyID()).get(slot);
		for (SlotPair slotPair : slotPairList) {
			Slot s = slotPair.getFirst();
			if (s.equals(slot))
				s = slotPair.getSecond();
			Node n = getNode(s);
			if (n != null && !n.equals(node)) {
				NodePair nodePair = getNodePair(n, node);
				nodePair.addTraffic(slotPair.getTraffic());
			}
		}
		
		slotToNodeMap.put(slot, node);
		List<Slot> slotList = nodeToSlotsMap.get(node);
		if (slotList == null) {
			slotList = new ArrayList<Slot>();
			nodeToSlotsMap.put(node, slotList);
		}
		slotList.add(slot);
		List<Node> nodeList = topologyToNodesMap.get(slot.getTopology());
		if (nodeList == null) {
			nodeList = new ArrayList<Node>();
			topologyToNodesMap.put(slot.getTopology(), nodeList);
		}
		if (!nodeList.contains(node))
			nodeList.add(node);
	}
	
	/**
	 * invoked when a slot is removed from a node; updates the inter-slot traffic stats
	 * @param node
	 * @param slot
	 */
	public void slotRemoved(Node node, Slot slot) {
		List<SlotPair> slotPairList = compiledInterSlotTrafficMap.get(slot.getTopology().getTopologyID()).get(slot);
		for (SlotPair slotPair : slotPairList) {
			Slot s = slotPair.getFirst();
			if (s.equals(slot))
				s = slotPair.getSecond();
			Node n = getNode(s);
			if (n != null && !n.equals(node)) {
				NodePair nodePair = getNodePair(n, node);
				nodePair.removeTraffic(slotPair.getTraffic());
			}
		}
		
		slotToNodeMap.remove(slot);
		nodeToSlotsMap.get(node).remove(slot);
		boolean nodeStillContainsSlotsOfThatTopology = false;
		for (Slot otherSlot : node.getSlotList())
			if (otherSlot.getTopology().equals(slot.getTopology())) {
				nodeStillContainsSlotsOfThatTopology = true;
				break;
			}
		if (!nodeStillContainsSlotsOfThatTopology)
			topologyToNodesMap.get(slot.getTopology()).remove(node);
	}
	
	/**
	 * @param n1
	 * @param n2
	 * @return the node pair identified by given nodes; the order the nodes are provided is not relevant; in case such pair doesn't exist, it gets created
	 */
	private NodePair getNodePair(Node n1, Node n2) {
		for (NodePair nodePair : interNodeTrafficList)
			if ((nodePair.getFirst().equals(n1) && nodePair.getSecond().equals(n2)) ||
				(nodePair.getFirst().equals(n2) && nodePair.getSecond().equals(n1)) )
				return nodePair;
		NodePair nodePair = new NodePair(n1, n2);
		interNodeTrafficList.add(nodePair);
		return nodePair;
	}
	
	/**
	 * this method should merge all the lists in interSlotTrafficMap
	 * @return the list of communicating slot pairs, sorted by traffic descending
	 */
	public List<SlotPair> getInterSlotTrafficList() {
		List<SlotPair> interSlotTrafficList = new ArrayList<SlotPair>();
		for (String topologyID : interSlotTrafficMap.keySet()) {
			List<SlotPair> slotPairList = interSlotTrafficMap.get(topologyID);
			for (SlotPair slotPair : slotPairList) {
				// TODO use binary search for determining the right index
				int index = 0;
				for (; index < interSlotTrafficList.size() - 1 && slotPair.getTraffic() < interSlotTrafficList.get(index).getTraffic(); index++);
				if (index < interSlotTrafficList.size() - 1)
					interSlotTrafficList.add(index, slotPair);
				else
					interSlotTrafficList.add(slotPair);
			}
		}
		return interSlotTrafficList;
	}
	
	/**
	 * @param topologyID
	 * @return the value in tuple/s of the inter-slot traffic for the given topology
	 */
	public int computeInterSlotTraffic(String topologyID) {
		int interSlotTraffic = 0;
		List<SlotPair> trafficList = interSlotTrafficMap.get(topologyID);
		for (SlotPair slotPair : trafficList)
			interSlotTraffic += slotPair.getTraffic();
		return interSlotTraffic;
	}	

	/**
	 * @param s1
	 * @param s2
	 * @return the list of nodes containing either slot
	 */
	public List<Node> getContainingNodeList(Slot s1, Slot s2) {
		List<Node> nodeList = new ArrayList<Node>();
		Node n1 = getNode(s1);
		if (n1 != null)
			nodeList.add(n1);
		Node n2 = getNode(s2);
		if (n2 != null && !n2.equals(n1))
			nodeList.add(n2);
		return nodeList;
	}
	
	/**
	 * @return a map node -> list of assigned slots
	 */
	public Map<Node, List<Slot>> getAssignments() {
		if (slotToNodeMap.keySet().isEmpty())
			return null;
		return nodeToSlotsMap;
	}
	
	/**
	 * @param topology
	 * @return the list of nodes where the slots of given topology are assigned
	 */
	public List<Node> getNodeList(Topology topology) {
		return topologyToNodesMap.get(topology);
	}
	
	/**
	 * @return the value in tuple/s of the inter-node traffic
	 */
	public int computeInterNodeTraffic() {
		if (interNodeTrafficList.isEmpty())
			return -1;
		int totalTraffic = 0;
		for (NodePair nodePair : interNodeTrafficList)
			totalTraffic += nodePair.getTraffic();
		return totalTraffic;
	}
}
