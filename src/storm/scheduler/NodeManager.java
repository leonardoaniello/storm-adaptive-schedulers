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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;

public class NodeManager {

	/**
	 * node name -> node instance
	 */
	private Map<String, Node> nodeMap;
	
	/**
	 * topology ID -> max number of slots per node for that topology
	 */
	private Map<String, Integer> maxSlotsPerNodePerTopologyMap;
	
	private Logger logger;
	
	public NodeManager(List<Topology> topologyList, Cluster cluster) {
		logger = Logger.getLogger(NodeManager.class);
		try {
			nodeMap = DataManager.getInstance().getNodes();
			for (Node node : nodeMap.values()) {
				logger.debug("Configuring node " + node + "...");
				List<SupervisorDetails> supervisorList = cluster.getSupervisorsByHost(node.getName());
				logger.debug("Supervisors list for " + node.getName() + ": " + Utils.collectionToString(supervisorList));
				if (supervisorList != null && !supervisorList.isEmpty()) {
					SupervisorDetails supervisor = supervisorList.get(0);
					Map<Integer, Integer> portTraceMap = new HashMap<Integer, Integer>();
					for (Integer port : cluster.getAvailablePorts(supervisor))
						portTraceMap.put(port, port);
					for (Integer port : cluster.getUsedPorts(supervisor))
						portTraceMap.put(port, port);
					node.setTotalSlots(portTraceMap.keySet().size());
					logger.info("Node: " + node);
				}
			}
			
			maxSlotsPerNodePerTopologyMap = new HashMap<String, Integer>();
			for (Topology topology : topologyList) {
				int slotsPerNode = topology.getMaxNumberOfSlotsPerNode(nodeMap.keySet().size());
				maxSlotsPerNodePerTopologyMap.put(topology.getTopologyID(), slotsPerNode);
				logger.info("Max number of slots per node for topology " + topology.getTopologyID() + ": " + slotsPerNode);
			}
		} catch (Exception e) {
			logger.error("Error occurred retrieving nodes from DB", e);
			throw new RuntimeException("Error occurred retrieving nodes from DB", e);
		}
	}
	
	public void setTotalSlots(String nodeName, int totalSlots) {
		Node node = nodeMap.get(nodeName);
		if (node != null)
			node.setTotalSlots(totalSlots);
	}
	
	public Node getNode(String name) {
		return nodeMap.get(name);
	}
	
	public Node getLeastLoadedNode(Slot slot) {
		return getLeastLoadedNode(1, slot.getLoad(), slot.getTopology().getTopologyID(), null);
	}
	
	public Node getLeastLoadedNode(Slot s1, Slot s2) {
		// we're assuming that the slots are for the same topology
		return getLeastLoadedNode(2, s1.getLoad() + s2.getLoad(), s1.getTopology().getTopologyID(), null);
	}
	
	public Node getLeastLoadedNode(SlotPair slotPair) {
		long load = Math.max(slotPair.getFirst().getLoad(), slotPair.getSecond().getLoad());
		return getLeastLoadedNode(1, load, slotPair.getFirst().getTopology().getTopologyID(), null);
	}
	
	public Node getUnusedNode(List<Node> usedNodeList, Slot slot) {
		return getLeastLoadedNode(1, slot.getLoad(), slot.getTopology().getTopologyID(), usedNodeList);
	}

	private Node getLeastLoadedNode(int slotCount, long totalLoad, String topologyId, List<Node> nodeBlackList) {
		Node leastLoaded = null;
		for (Node node : nodeMap.values()) {
			if (node.getAvailableSlotCount() >= slotCount && node.canSustainLoad(totalLoad) && // can accept the slot
				node.getTopologySlotCount(topologyId) + slotCount <= maxSlotsPerNodePerTopologyMap.get(topologyId) && // constraint on the number of slots per node for that topology
				(nodeBlackList == null || !nodeBlackList.contains(node)) && // node not blacklisted
				(leastLoaded == null || leastLoaded.getLoad() > node.getLoad())) // least loaded
			{
				// logger.info("+++ getLeastLoadedNode - BEGIN");
				// logger.info("input> slotCount: " + slotCount + ", totalLoad: " + totalLoad + ", topologyId: " + topologyId);
				// logger.info("number of topology slots in node " + node + ": " + node.getTopologySlotCount(topologyId));
				// logger.info("max number of slots per node for topology " + topologyId + ": " + maxSlotsPerNodePerTopologyMap.get(topologyId));
				leastLoaded = node;
				// logger.info("+++ getLeastLoadedNode - END");
			}
		}
		return leastLoaded;
	}
	
	public int getNodeCount() {
		return nodeMap.values().size();
	}
	
	public Collection<Node> getNodes() {
		return nodeMap.values();
	}
}
