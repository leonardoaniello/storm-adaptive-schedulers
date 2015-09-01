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

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class OfflineScheduler implements IScheduler {
	
	private Logger logger = Logger.getLogger(OfflineScheduler.class);
	private AssignmentTracker assignmentTracker = new AssignmentTracker();

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		logger.info("Offline Scheduler");
		logger.info("+++++++++++++++++++++++++++");
		
		try {
			for (TopologyDetails topology : topologies.getTopologies()) {
				logger.debug("Checking topology " + topology.getName() + " (id: " + topology.getId() + ")");
				if (cluster.needsScheduling(topology)) {
					logger.debug("Topology " + topology.getId() + " needs rescheduling");
					// check if required data structures are included in topology conf
					@SuppressWarnings("unchecked") List<String> componentList = (List<String>)topology.getConf().get("components");
					// component -> list of input components
					@SuppressWarnings("unchecked") Map<String, List<String>> streamMap = (Map<String, List<String>>)topology.getConf().get("streams");
					if (componentList != null && streamMap != null) {
						logger.debug("components: " + collectionToString(componentList));
						logger.debug("streams: " + mapToString(streamMap));
						
						float alfa = 0;
						if (topology.getConf().get("alfa") != null)
							alfa = Float.parseFloat((String)topology.getConf().get("alfa"));
						float beta = 1;
						if (topology.getConf().get("beta") != null)
							beta = Float.parseFloat((String)topology.getConf().get("beta"));
						float epsilon = 0.5f;
						if (topology.getConf().get("epsilon") != null)
							epsilon = Float.parseFloat((String)topology.getConf().get("epsilon"));
						logger.debug("alfa: " + alfa + ", beta: " + beta + ", epsilon: " + epsilon);
						
						// prepare the slots
						logger.debug("Number of workers: " + topology.getNumWorkers());
						List<List<ExecutorDetails>> slotList = new ArrayList<List<ExecutorDetails>>();
						for (int i = 0; i < topology.getNumWorkers(); i++)
							slotList.add(new ArrayList<ExecutorDetails>());
						
						// compute how many executors at most can be assigned to a single slot 
						int executorCount = topology.getExecutors().size();
						int min = (int)Math.ceil((double)executorCount/slotList.size());
						int max = executorCount - slotList.size() + 1;
						int maxExecutorPerSlot = min + (int)Math.ceil(alfa * (max - min));
						logger.debug("Maximum number of executors per slot: " + maxExecutorPerSlot);
						
						// component -> list of its executors (already populated)
						Map<String, List<ExecutorDetails>> componentToExecutorMap = cluster.getNeedsSchedulingComponentToExecutors(topology);
						
						// executor -> index of the slot where it is allocated (to be populated)
						Map<ExecutorDetails, Integer> executorToSlotMap = new HashMap<ExecutorDetails, Integer>();
						
						// iterate through the components, upstream to downstream
						for (String component : componentList) {
							logger.debug("Check for primary slots for component " + component);
							List<String> inputComponentList = streamMap.get(component);
							logger.debug("input components: " + Utils.collectionToString(inputComponentList));
							List<ExecutorDetails> executorList = componentToExecutorMap.get(component);
							logger.debug("executors: " + Utils.collectionToString(executorList));
							
							// identify primary slots and secondary slots
							List<Integer> primarySlotList = new ArrayList<Integer>();
							List<Integer> secondarySlotList = new ArrayList<Integer>();
							Map<Integer, Integer> slotToUseMap = new HashMap<Integer, Integer>(); // fictitious, just to quickly check whether a slot has already been used
							if (inputComponentList != null) {
								// for each input component, track the slots where related executors are currently assigned
								for (String inputComponent : inputComponentList) {
									logger.debug("Checking input component " + inputComponent);
									List<ExecutorDetails> inputExecutorList = componentToExecutorMap.get(inputComponent);
									logger.debug("executors for input component " + inputComponent + ": " + Utils.collectionToString(inputExecutorList));
									for (ExecutorDetails inputExecutor : inputExecutorList) {
										int slotIdx = executorToSlotMap.get(inputExecutor);
										slotToUseMap.put(slotIdx, 1);
										logger.debug("input executor " + inputExecutor + " is assigned to slot " + slotIdx + ", so this slot is a primary one");
									}
								}
							}
							
							// if a slot includes an executor of an input component, then it is a primary slot, otherwise a secondary
							for (int i = 0; i < slotList.size(); i++) {
								if (slotList.get(i).size() < maxExecutorPerSlot) {
									if (slotToUseMap.get(i) != null)
										primarySlotList.add(i);
									else
										secondarySlotList.add(i);
								}
							}
							
							/*
							 * promote a secondary slot to primary if it is still empty;
							 * this way, we ensure that all the slots are used;
							 * this is done after having already scheduled epsilon of the components
							 */
							logger.debug("this component index: " + componentList.indexOf(component) + ", index of component where to start forcing to use empty slots: " + (int)(epsilon * componentList.size()));
							if (componentList.indexOf(component) >= (int)(epsilon * componentList.size()) ) {
								List<Integer> slotToPromoteList = new ArrayList<Integer>();
								for (int secondarySlot : secondarySlotList)
									if (slotList.get(secondarySlot).isEmpty())
										slotToPromoteList.add(secondarySlot);
								for (Integer slotToPromote : slotToPromoteList) {
									secondarySlotList.remove(slotToPromote);
									primarySlotList.add(0, slotToPromote);
								}
							}
							
							logger.debug("Primary slots for component " + component + ": " + Utils.collectionToString(primarySlotList));
							logger.debug("Secondary slots for component " + component + ": " + Utils.collectionToString(secondarySlotList));
							
							int primaryIdx = 0;
							int secondaryIdx = 0;
							for (ExecutorDetails executor : executorList) {
								logger.debug("Assigning executor " + executor);
								// assign executors to slots in a round-robin fashion
								// if a primary slot is available (that is, enough available space for another executor), assign to the primary
								// otherwise, assign to a secondary
								int slotIdx = -1;
								while (!primarySlotList.isEmpty() && slotList.get(primarySlotList.get(primaryIdx)).size() == maxExecutorPerSlot) {
									logger.debug("Primary slot " + primarySlotList.get(primaryIdx) + " is full, remove it");
									primarySlotList.remove(primaryIdx);
									if (primaryIdx == primarySlotList.size()) {
										primaryIdx = 0;
										logger.debug("Reached the tail of primary slot list, point to the head");
									}
								}
								if (!primarySlotList.isEmpty()) {
									slotIdx = primarySlotList.get(primaryIdx);
									primaryIdx = (primaryIdx + 1) % primarySlotList.size();
								}
								
								if (slotIdx == -1) {
									logger.debug("No primary slot availble, choose a secondary slot");
									while (!secondarySlotList.isEmpty() && slotList.get(secondarySlotList.get(secondaryIdx)).size() == maxExecutorPerSlot) {
										logger.debug("Secondary slot " + secondarySlotList.get(secondaryIdx) + " is full, remove it");
										secondarySlotList.remove(secondaryIdx);
										if (secondaryIdx == secondarySlotList.size()) {
											secondaryIdx = 0;
											logger.debug("Reached the tail of secondary slot list, point to the head");
										}
									}
									if (!secondarySlotList.isEmpty()) {
										slotIdx = secondarySlotList.get(secondaryIdx);
										secondaryIdx = (secondaryIdx + 1) % secondarySlotList.size();
									}
								}
								
								if (slotIdx == -1)
									throw new Exception("Cannot assign executor " + executor + " to any slot");
								slotList.get(slotIdx).add(executor);
								executorToSlotMap.put(executor, slotIdx);
								logger.debug("Assigned executor " + executor + " to slot " + slotIdx);
							}
							
						} /* end for (String component : componentList) */
						
						// compute the number of nodes to use
						List<WorkerSlot> workerList = cluster.getAvailableSlots();
						NodeHelper nodeHelper = new NodeHelper(workerList, beta, slotList.size());

						// assign executors to slots using the proper number of nodes, in a round-robin fashion
						int i = 0;
						for (List<ExecutorDetails> slot : slotList) {
							WorkerSlot worker = nodeHelper.getWorker(i);
							cluster.assign(worker, topology.getId(), slot);
							logger.info("We assigned executors:" + collectionToString(slot) + " to slot: [" + worker.getNodeId() + ", " + worker.getPort() + "]");
							i++;
						}
					
					} else {
						
						logger.warn("No components and streams defined for topology " + topology);
						
					} /* end if (components != null && streams != null) */
					
				} /* end if (cluster.needsScheduling(topology)) */
				
			} /* end for (TopologyDetails topology : topologies.getTopologies()) */
		} catch (Exception e) {
			logger.error("An error occurred during the scheduling", e);
		}
		
		logger.info("---------------------------");
		logger.info("Calling EvenScheduler to schedule remaining executors...");
		new EvenScheduler().schedule(topologies, cluster);
		logger.info("Ok, EvenScheduler succesfully called");
		
		assignmentTracker.checkAssignment(topologies, cluster);
	}
	
	/**
	 * @param list
	 * @return the list in csv format
	 */
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
	
	private String mapToString(Map<String, List<String>> map) {
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
