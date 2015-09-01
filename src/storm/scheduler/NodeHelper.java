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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.WorkerSlot;

public class NodeHelper {
	
	private List<WorkerSlot> slotList;
	private Logger logger;
	
	public NodeHelper(List<WorkerSlot> workerList, float beta, int slotCount) {
		logger = Logger.getLogger(NodeHelper.class);
		Map<String, List<WorkerSlot>> nodeToSlotMap = new HashMap<String, List<WorkerSlot>>(); // node ID -> list of workers of that node
		List<String> nodeList = new ArrayList<String>();
		List<Integer> slotCountList = new ArrayList<Integer>();
		
		// partition slots into nodes
		for (WorkerSlot slot : workerList) {
			List<WorkerSlot> nodeSlotList = nodeToSlotMap.get(slot.getNodeId());
			if (nodeSlotList == null) {
				nodeSlotList = new ArrayList<WorkerSlot>();
				nodeToSlotMap.put(slot.getNodeId(), nodeSlotList);
			}
			nodeSlotList.add(slot);
		}
		logger.debug("Slots partitioned into nodes: " + mapToString(nodeToSlotMap));
		
		// populate nodeList and slotCountList lists, sorted by count desc
		for (String nodeId : nodeToSlotMap.keySet()) {
			int count = nodeToSlotMap.get(nodeId).size();
			int i = 0;
			for (; i < slotCountList.size() && slotCountList.get(i) > count; i++);
			nodeList.add(i, nodeId);
			slotCountList.add(i, count);
		}
		
		logger.debug("List of nodes and their available slots, sorted by slot count desc");
		for (int i = 0; i < nodeList.size(); i++)
			logger.debug(nodeList.get(i) + ": " + slotCountList.get(i) + " slots");
		
		// determine the number of nodes to use
		int sum = 0;
		int minNumberOfNodes = 0;
		while (sum < slotCount)
			sum += slotCountList.get(minNumberOfNodes++);
		int maxNumberOfNodes = Math.min(nodeList.size(), slotCount);
		int nodeToUseCount = minNumberOfNodes + (int)Math.ceil(beta * (maxNumberOfNodes - minNumberOfNodes));
		logger.debug("Min number of nodes: " + minNumberOfNodes + ", max number of nodes: " + maxNumberOfNodes + ", number of nodes to use: " + nodeToUseCount);
		
		// populate slotList
		slotList = new ArrayList<WorkerSlot>();
		int nodeIdx = 0;
		for (int i = 0; i < slotCount; i++) {
			while (nodeToSlotMap.get(nodeList.get(nodeIdx)).isEmpty())
				nodeIdx = (nodeIdx + 1) % nodeToUseCount;
			slotList.add( nodeToSlotMap.get(nodeList.get(nodeIdx)).remove(0) );
			nodeIdx = (nodeIdx + 1) % nodeToUseCount;
		}
		
		logger.debug("List of available slots to use, sorted according to a round-robin fashion based on the nodes to use");
		for (WorkerSlot slot : slotList)
			logger.debug(slot);
	}
	
	/**
	 * @param i
	 * @return the WorkerSlot for the i-th slot
	 */
	public WorkerSlot getWorker(int i) {
		return slotList.get(i);
	}
	
	private String collectionToString(Collection<?> list) {
		if (list == null)
			return "null";
		if (list.isEmpty())
			return "<empty list>";
		StringBuffer sb = new  StringBuffer();
		int i = 0;
		for (Object item : list) {
			sb.append(item.toString());
			if (i < list.size() - 1)
				sb.append(", ");
			i++;
		}
		return sb.toString();
	}
	
	private String mapToString(Map<String, List<WorkerSlot>> map) {
		if (map == null)
			return "null";
		if (map.keySet().isEmpty())
			return "<empty map>";
		StringBuffer sb = new  StringBuffer();
		int i = 0;
		for (Object key : map.keySet()) {
			sb.append(key.toString() + " -> (");
			sb.append(collectionToString(map.get(key)) + ")");
			if (i < map.keySet().size() - 1)
				sb.append(", ");
			i++;
		}
		return sb.toString();
	}
}
