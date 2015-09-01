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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class OnlineScheduler implements IScheduler {
	
	private static final int DEFAULT_RESCHEDULE_TIMEOUT = 180; // s
	
	private Logger logger = Logger.getLogger(OnlineScheduler.class);
	private AssignmentTracker assignmentTracker = new AssignmentTracker();
	
	private long lastRescheduling;
	
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		logger.info("Online Scheduler");
		logger.info("+++++++++++++++++++++++++++");
		if (!topologies.getTopologies().isEmpty()) {
			int rescheduleTimeout = DEFAULT_RESCHEDULE_TIMEOUT;
			for (TopologyDetails topology : topologies.getTopologies()) {
				rescheduleTimeout = Integer.parseInt(topology.getConf().get(Utils.RESCHEDULE_TIMEOUT).toString());
			}
			long now = System.currentTimeMillis();
			long elapsedTime = (now - lastRescheduling) / 1000; // s
			if (lastRescheduling == 0 || elapsedTime >= rescheduleTimeout)
				doSchedule(topologies, cluster);
			else
				logger.info("It's not time to reschedule yet, " + elapsedTime + " seconds have passed, other " + (rescheduleTimeout - elapsedTime) + " seconds have to pass");
		}

		logger.info("---------------------------");
		
		logger.info("Calling EvenScheduler to schedule remaining executors...");
		new EvenScheduler().schedule(topologies, cluster);
		logger.info("Ok, EvenScheduler succesfully called");
		
		assignmentTracker.checkAssignment(topologies, cluster);
	}
	
	private void doSchedule(Topologies topologies, Cluster cluster) {
		try {
			// get topologies from DBs
			List<String> dbTopologies = DataManager.getInstance().getTopologies();
			logger.info("DB Topologies: " + Utils.collectionToString(dbTopologies));
			
			// get topologies from Storm and identify topologies to be deleted
			List<String> topologiesToBeRemoved = new ArrayList<String>(dbTopologies);
			List<String> stormTopologyList = new ArrayList<String>();
			int trafficImprovement = 0;
			for (TopologyDetails topology : topologies.getTopologies()) {
				topologiesToBeRemoved.remove(topology.getId());
				stormTopologyList.add(topology.getId());
				logger.debug("Configuration of topology " + topology.getId());
				for (Object key : topology.getConf().keySet())
					logger.debug("- " + key + ": " + topology.getConf().get(key));
				trafficImprovement = Integer.parseInt(topology.getConf().get(Utils.TRAFFIC_IMPROVEMENT).toString());
			}
			logger.info("Storm Topologies: " + Utils.collectionToString(stormTopologyList));
			
			dbTopologies.removeAll(topologiesToBeRemoved);
			logger.info("Topologies to be removed from DB: " + Utils.collectionToString(topologiesToBeRemoved));
			
			// remove topologies from DB
			if (!topologiesToBeRemoved.isEmpty()) {
				DataManager.getInstance().removeTopologies(topologiesToBeRemoved);
				logger.info("Topologies succesfully removed from DB");
			}
			
			// compute best scheduling
			TrafficManager.getInstance().clear();
			computeBestScheduling(dbTopologies, topologies, cluster);
			Map<Node, List<Slot>> bestAssignment = TrafficManager.getInstance().getAssignments();
			int bestInterNodeTraffic = TrafficManager.getInstance().computeInterNodeTraffic();
			int currentInterNodeTraffic = DataManager.getInstance().getCurrentInterNodeTraffic();
			List<Node> overloadedNodeList = DataManager.getInstance().getOverloadedNodes();
			
			// check if a rescheduling is required
			logger.info("These nodes are currently overloaded: " + Utils.collectionToString(overloadedNodeList));
			logger.info("Currently, the inter-node traffic is " + currentInterNodeTraffic + " tuple/s");
			
			if (bestAssignment != null) {
				logger.info("The best assignment can lead to an inter-node traffic of " + bestInterNodeTraffic + " tuple/s");
				boolean reschedulingDueToOverloading = false;
				boolean reschedulingDueToInterNodeTraffic = false;
				if (!overloadedNodeList.isEmpty()) {
					logger.info("Check how the new assignment can offload some of the currently overloaded nodes");
					for (Node node : overloadedNodeList) {
						Node nodeAfterTheAssignment = null;
						for (Node n : bestAssignment.keySet())
							if (n.equals(node)) {
								nodeAfterTheAssignment = n;
								break;
							}
						if (nodeAfterTheAssignment != null) {
							logger.info("Node " + node.getName() + " currenlty has a load of " + node.getLoad() + " Hz/s, and the assignment can lead to " + nodeAfterTheAssignment.getLoad());
							if (node.getLoad() > nodeAfterTheAssignment.getLoad())
								reschedulingDueToOverloading = true;
						} else {
							logger.warn("Node " + node.getName() + " currenlty has a load of " + node.getLoad() + " Hz/s, but it doesn't appear in the new assignment");
						}
					}
				}
				if (reschedulingDueToOverloading)
					logger.info("A rescheduling is required to offload currently overloaded nodes");
				int trafficThreshold = (int)((float)currentInterNodeTraffic * (1 - (float)trafficImprovement / 100));
				logger.info("Minimum traffic threshold is " + trafficThreshold + " tuple/s");
				if (trafficThreshold  >= bestInterNodeTraffic) {
					logger.info("A rescheduling is required to lower inter-node traffic");
					reschedulingDueToInterNodeTraffic = true;
				}
				if (reschedulingDueToInterNodeTraffic || reschedulingDueToOverloading) {
					logger.info("Let's apply the best assignment!!");
					lastRescheduling = System.currentTimeMillis();
					
					// free all available slots
					for (SupervisorDetails supervisor : cluster.getSupervisors().values()) {
						List<Integer> usedPorts = cluster.getUsedPorts(supervisor);
						for (int usedPort : usedPorts)
							cluster.freeSlot(new WorkerSlot(supervisor.getId(), usedPort));
					}
					
					for (Node node : bestAssignment.keySet()) {
						SupervisorDetails supervisor = cluster.getSupervisorsByHost(node.getName()).get(0);
						List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
						int slotIndex = 0;
						for (Slot slot : bestAssignment.get(node)) {
							// all the executors in this slots belong to a specific topology;
							// they all have to be put in a unique slot and assigned to the node in this scope
							logger.info("Assigning executors of slot " + slot);
							String topology = slot.getTopology().getTopologyID();
							List<ExecutorDetails> executorList = new ArrayList<ExecutorDetails>();
							
							// here a match is required to link scheduler executors to storm executors
							Collection<ExecutorDetails> allTopologyExecutors = topologies.getById(topology).getExecutors();
							for (Executor executor : slot.getExecutors()) {
								for (ExecutorDetails executorDetails : allTopologyExecutors) {
									if (executor.match(executorDetails)) {
										executorList.add(executorDetails);
										break;
									}
								}
							}
							
							cluster.assign(availableSlots.get(slotIndex), topology, executorList);
							logger.info("We assigned executors:" + Utils.collectionToString(executorList) + " to slot: [" + availableSlots.get(slotIndex).getNodeId() + ", " + availableSlots.get(slotIndex).getPort() + "]");
			                slotIndex++;
			                
						} /* end for (Slot slot : bestAssignment.get(node)) */
						
					} /* end for (Node node : bestAssignment.keySet()) */
					
					logger.info("Rescheduling completed, now reset all stats from DB");
					DataManager.getInstance().removeTopologies(dbTopologies);
					
				} /* end if (reschedulingDueToInterNodeTraffic || reschedulingDueToOVerloading) */
			} else {
				logger.info("No assignment has been simulated");
			}
			
		} catch (Throwable t) {
			logger.error("Error occurred during scheduling", t);
		}
	}
	
	/*private void checkAckers(String topologyID, Cluster cluster) throws Exception {
		Set<ExecutorDetails> executorSet = cluster.getAssignmentById(topologyID).getExecutors();
		Map<ExecutorDetails, WorkerSlot> assignmentsMap = cluster.getAssignmentById(topologyID).getExecutorToSlot();
		for (ExecutorDetails executor : executorSet) {
			if (executor.getStartTask() == Utils.ACKER_TAKS_ID && executor.getEndTask() == Utils.ACKER_TAKS_ID) {
				String nodeId = assignmentsMap.get(executor).getNodeId();
				String host = cluster.getSupervisorById(nodeId).getHost();
				DataManager.getInstance().storeAcker(topologyID, host);
			}
		}
	}*/
	
	private void computeBestScheduling(List<String> dbTopologies, Topologies stormTopologies, Cluster cluster) throws Exception {
		
		logger.info("-- First phase --");
		List<Topology> topologyList = new ArrayList<Topology>();
		for (String topologyID : dbTopologies) {
			logger.info("Topology ID: " + topologyID);
			TopologyDetails topologyDetails = stormTopologies.getById(topologyID);
			Topology topology = new Topology(topologyDetails);
			topology.setTotalLoad(DataManager.getInstance().getTotalLoad(topologyID));
			topologyList.add(topology);
			// logger.info("Max number of executors per slot: " + topology.getMaxExecutorsPerSlot() + ", max load per slot: " + topology.getMaxLoadForASlot() + " cycle/s, slot count: " + topology.getSlots().size() + ", total load: " + topology.getTotalLoad() + " cycle/s, alfa: " + topology.getAlfa() + ", beta: " + topology.getBeta());
			logger.info("Max number of executors per slot: " + topology.getMaxExecutorsPerSlot() + ", slot count: " + topology.getSlots().size() + ", total load: " + topology.getTotalLoad() + " cycle/s, alfa: " + topology.getAlfa() + ", beta: " + topology.getBeta());
			/*if (Integer.parseInt(topologyDetails.getConf().get(Config.TOPOLOGY_ACKER_EXECUTORS).toString() ) != 0)
				checkAckers(topologyID, cluster);*/
			List<ExecutorPair> interExecutorTrafficList = TrafficManager.getInstance().getInterExecutorTrafficList(topologyID);
			logger.info("Inter-executor traffic stats: " + Utils.collectionToString(interExecutorTrafficList));
			if (interExecutorTrafficList.isEmpty()) {
				logger.info("Traffic stats are not complete yet, skip this topology");
			} else {
				for (ExecutorPair executorPair : interExecutorTrafficList) {
					logger.debug("Executor pair: " + executorPair);
					List<Slot> slotList = topology.getContainingSlotList(executorPair.getSource(), executorPair.getDestination());
					logger.debug("Slots that already contain either executors: " + Utils.collectionToString(slotList));
					if (slotList.isEmpty()) {
						logger.debug("Both executors have not been assigned yet, try to add them to the least loaded slot");
						Slot leastLoadedSlot = topology.getLeastLoadedSlot(executorPair.getSource(), executorPair.getDestination());
						if (leastLoadedSlot != null) {
							logger.debug("Least loaded slot able to get both the executors: " + leastLoadedSlot);
							leastLoadedSlot.assign(executorPair.getSource());
							leastLoadedSlot.assign(executorPair.getDestination());
							logger.info("Executors " + executorPair.getSource() + " and " + executorPair.getDestination() + " assigned to slot " + leastLoadedSlot);
						} else {
							logger.debug("No slot exists that can get both the executors, assign them to distinct slots");
							leastLoadedSlot = topology.getLeastLoadedSlot(executorPair.getSource());
							if (leastLoadedSlot == null)
								throw new RuntimeException("Cannot find a slot able to get executor " + executorPair.getSource() + " for topology " + topology);
							logger.debug("Least loaded slot for source executor: " + leastLoadedSlot);
							leastLoadedSlot.assign(executorPair.getSource());
							logger.info("Executor " + executorPair.getSource() + " assigned to slot " + leastLoadedSlot);
							if (leastLoadedSlot.canAccept(executorPair.getDestination())) {
								logger.debug("After having added executor " + executorPair.getSource() + ", the slot " + leastLoadedSlot + " can also get the executor " + executorPair.getDestination());
								leastLoadedSlot.assign(executorPair.getDestination());
							} else {
								logger.debug("After having added executor " + executorPair.getSource() + ", the slot " + leastLoadedSlot + " cannot get the executor " + executorPair.getDestination());
								leastLoadedSlot = topology.getLeastLoadedSlot(executorPair.getDestination());
								if (leastLoadedSlot == null)
									throw new RuntimeException("Cannot find a slot able to get executor " + executorPair.getDestination() + " for topology " + topology);
								logger.debug("Least loaded slot for destination executor: " + leastLoadedSlot);
								leastLoadedSlot.assign(executorPair.getDestination());
							}
							logger.info("Executor " + executorPair.getDestination() + " assigned to slot " + leastLoadedSlot);
						}
					} else {
						logger.debug("Some executor has been already assigned, compute the best assignment using the slot(s) found before and the least loaded one");
						Slot leastLoadedSlot = topology.getLeastLoadedSlot(executorPair);
						logger.debug("Least loaded slot: " + leastLoadedSlot);
						if (leastLoadedSlot != null && !slotList.contains(leastLoadedSlot))
							slotList.add(leastLoadedSlot);
						logger.debug("Slots to use: " + Utils.collectionToString(slotList));
						
						logger.debug("Remove source and destination from the slots they are currently assigned to");
						for (Slot slot : slotList) {
							if (slot.contains(executorPair.getSource()))
								slot.remove(executorPair.getSource());
							if (slot.contains(executorPair.getDestination()))
								slot.remove(executorPair.getDestination());
						}
						logger.debug("Slots to use after such removals: " + Utils.collectionToString(slotList));
						
						logger.debug("Check every possible combination");
						Slot bestSlotForSource = null;
						Slot bestSlotForDestination = null;
						int minInterSlotTraffic = -1;
						for (Slot slotForSource : slotList) {
							for (Slot slotForDestination : slotList) {
								logger.debug("Assigning executor " + executorPair.getSource() + " to slot " + slotForSource + " and executor " + executorPair.getDestination() + " to slot " + slotForDestination + "...");
								boolean assignmentOk = true;
								if (slotForSource.canAccept(executorPair.getSource())) {
									slotForSource.assign(executorPair.getSource());
								} else {
									logger.debug("Slot " + slotForSource + " is imbalanced, cannot be used to add more executors");
									assignmentOk = false;
								}
								
								if (slotForDestination.canAccept(executorPair.getDestination())) {
									slotForDestination.assign(executorPair.getDestination());
								} else {
									logger.debug("Slot " + slotForDestination + " is imbalanced, cannot be used to add more executors");
									assignmentOk = false;
								}
								
								if (assignmentOk) {
									int interSlotTraffic = TrafficManager.getInstance().computeInterSlotTraffic(topologyID);
									logger.debug("...the inter-slot traffic is " + interSlotTraffic + " tuple/s");
									if (minInterSlotTraffic == -1 || interSlotTraffic < minInterSlotTraffic) {
										bestSlotForSource = slotForSource;
										bestSlotForDestination = slotForDestination;
										minInterSlotTraffic = interSlotTraffic;
									}
								}
								if (slotForSource.contains(executorPair.getSource()))
									slotForSource.remove(executorPair.getSource());
								if (slotForDestination.contains(executorPair.getDestination()))
									slotForDestination.remove(executorPair.getDestination());
							}
						}
						if (bestSlotForSource == null || bestSlotForDestination == null)
							throw new Exception("Cannot find a possible assignment of executors " + executorPair.getSource() + " and " + executorPair.getDestination() + " to slots " + Utils.collectionToString(slotList));
						logger.debug("The best assignment is executor " + executorPair.getSource() + " to slot " + bestSlotForSource + " and executor " + executorPair.getDestination() + " to slot " + bestSlotForDestination + ", with inter-slot traffic " + minInterSlotTraffic + " tuple/s");
						bestSlotForSource.assign(executorPair.getSource());
						bestSlotForDestination.assign(executorPair.getDestination());
						logger.info("Executor " + executorPair.getSource() + " assigned to slot " + bestSlotForSource);
						logger.info("Executor " + executorPair.getDestination() + " assigned to slot " + bestSlotForDestination);
					} /* end if (!slotList.isEmpty()) */
					
					logger.debug("Assignment of executors " + executorPair + " completed");
					
				} /* end for (ExecutorPair executorPair : executorPairList) */
				
				logger.info("Current assignment: " + Utils.collectionToString(topology.getSlots()));
				logger.info("Check for empty slots");
				List<Slot> emptySlotList = topology.getEmptySlots();
				if (emptySlotList.isEmpty()) {
					logger.info("No empty slots, the assignment is succesfully completed");
				} else {
					logger.info("Empty slots: " + Utils.collectionToString(emptySlotList));
					List<Slot> usedSlotList = topology.getUsedSlots();
					for (Slot emptySlot : emptySlotList) {
						logger.debug("Find an executor to assign to slot " + emptySlot);
						Executor bestExecutor = null;
						Slot bestSlot = null;
						int bestInterSlotTraffic = -1;
						for (Slot usedSlot : usedSlotList) {
							if (usedSlot.getExecutors().size() > 1) {
								logger.debug("Check the executors of slot " + usedSlot);
								List<Executor> executorList = new ArrayList<Executor>( usedSlot.getExecutors() );
								for (Executor executor : executorList) {
									usedSlot.remove(executor);
									emptySlot.assign(executor);
									int interSlotTraffic = TrafficManager.getInstance().computeInterSlotTraffic(topologyID);
									logger.debug("Moving executor " + executor + ", the inter-slot traffic is " + interSlotTraffic + " tuple/s");
									if (bestInterSlotTraffic == -1 || interSlotTraffic < bestInterSlotTraffic) {
										bestExecutor = executor;
										bestSlot = usedSlot;
										bestInterSlotTraffic = interSlotTraffic;
									}
									emptySlot.remove(executor);
									usedSlot.assign(executor);
								}
							}
						}
						if (bestSlot != null) {
							logger.debug("The best assignment is moving executor " + bestExecutor + " from slot " + bestSlot + " to slot " + emptySlot + " with an inter-slot traffic of " + bestInterSlotTraffic + " tuple/s");
							bestSlot.remove(bestExecutor);
							emptySlot.assign(bestExecutor);
							logger.info("Executor " + bestExecutor + " moved from slot " + bestSlot + " to slot " + emptySlot);
						} else {
							logger.warn("Cannot find an executor to move to slot " + emptySlot);
						}
					} /* end for (Slot emptySlot : emptySlotList) */
				}
				logger.info("Next assignment: " + Utils.collectionToString(topology.getSlots()));
				
				TrafficManager.getInstance().compileInterSlotTraffic();
				
			} /* end if (!executorPairList.isEmpty()) */
			
			logger.info("Assignments of executors for topology " + topologyID + " completed");
			
		} /* end for (String topologyID : dbTopologies) */
		logger.info("First phase completed!");
		List<SlotPair> interSlotTrafficList = TrafficManager.getInstance().getInterSlotTrafficList();
		logger.info("Inter-slot traffic stats: " + Utils.collectionToString(interSlotTrafficList));
		
		// second phase
		logger.info("-- Second phase --");
		NodeManager nodeManager = new NodeManager(topologyList, cluster);
		if (nodeManager.getNodeCount() == 0) {
			logger.info("No nodes have been configured yet, cannot determine any scheduling");
		} else {
			for (SlotPair slotPair : interSlotTrafficList) {
				logger.info("Slot pair: " + slotPair);
				List<Node> nodeList = TrafficManager.getInstance().getContainingNodeList(slotPair.getFirst(), slotPair.getSecond());
				if (nodeList.isEmpty()) {
					logger.debug("Both slots have not been assigned yet, try to add them to the least loaded node");
					Node leastLoadedNode = nodeManager.getLeastLoadedNode(slotPair.getFirst(), slotPair.getSecond());
					if (leastLoadedNode != null) {
						logger.debug("Least loaded node able to get both the slots: " + leastLoadedNode);
						leastLoadedNode.assign(slotPair.getFirst());
						leastLoadedNode.assign(slotPair.getSecond());
						logger.info("Slots " + slotPair.getFirst() + " and " + slotPair.getSecond() + " assigned to node " + leastLoadedNode + " (Slots of topology " + slotPair.getFirst().getTopology().getTopologyID() + " in this node: " + leastLoadedNode.getTopologySlotCount(slotPair.getFirst().getTopology().getTopologyID()));
					} else {
						logger.debug("No node exists that can get both the slots, assign them to distinct nodes");
						
						leastLoadedNode = nodeManager.getLeastLoadedNode(slotPair.getFirst());
						if (leastLoadedNode == null)
							throw new RuntimeException("Cannot find a node able to sustain the load of the slot " + slotPair.getFirst());
						logger.debug("Assign slot " + slotPair.getFirst() + " to node " + leastLoadedNode);
						leastLoadedNode.assign(slotPair.getFirst());
						logger.debug("Least loaded node after such assignment: " + leastLoadedNode);
						logger.info("Slot " + slotPair.getFirst() + " assigned to node " + leastLoadedNode + " (Slots of topology " + slotPair.getFirst().getTopology().getTopologyID() + " in this node: " + leastLoadedNode.getTopologySlotCount(slotPair.getFirst().getTopology().getTopologyID()));
						
						leastLoadedNode = nodeManager.getLeastLoadedNode(slotPair.getSecond());
						if (leastLoadedNode == null)
							throw new RuntimeException("Cannot find a node able to sustain the load of the slot " + slotPair.getSecond());
						logger.debug("Assign slot " + slotPair.getSecond() + " to node " + leastLoadedNode);
						leastLoadedNode.assign(slotPair.getSecond());
						logger.debug("Least loaded node after such assignment: " + leastLoadedNode);
						logger.info("Slot " + slotPair.getSecond() + " assigned to node " + leastLoadedNode + " (Slots of topology " + slotPair.getSecond().getTopology().getTopologyID() + " in this node: " + leastLoadedNode.getTopologySlotCount(slotPair.getSecond().getTopology().getTopologyID()));
					}
				} else {
					logger.debug("Either slot has been already assigned to nodes " + Utils.collectionToString(nodeList));
					Node leastLoadedNode = nodeManager.getLeastLoadedNode(slotPair);
					logger.debug("The least loaded node able to sustain the traffic of the slot with the lowest traffic is " + leastLoadedNode);
					if (leastLoadedNode != null && !nodeList.contains(leastLoadedNode))
						nodeList.add(leastLoadedNode);
					logger.debug("Nodes to use: " + Utils.collectionToString(nodeList));
					logger.debug("Remove slots " + slotPair.getFirst() + " and " + slotPair.getSecond() + " from these nodes");
					for (Node node : nodeList) {
						if (node.contains(slotPair.getFirst()))
							node.remove(slotPair.getFirst());
						if (node.contains(slotPair.getSecond()))
							node.remove(slotPair.getSecond());
					}
					logger.debug("Nodes after such removals: " + Utils.collectionToString(nodeList));
					
					logger.debug("Check every possible combination");
					Node bestNodeForFirst = null;
					Node bestNodeForSecond = null;
					int minInterNodeTraffic = -1;
					for (Node nodeForFirst : nodeList) {
						for (Node nodeForSecond : nodeList) {
							logger.debug("Assigning slot " + slotPair.getFirst() + " to node " + nodeForFirst + " and slot " + slotPair.getSecond() + " to node " + nodeForSecond + "...");
							boolean assignmentOK = true;
							
							if (nodeForFirst.canAssign(slotPair.getFirst())) {
								nodeForFirst.assign(slotPair.getFirst());
							} else {
								logger.debug("Cannot assign slot " + slotPair.getFirst() + " to noe " + nodeForFirst);
								assignmentOK = false;
							}
							
							if (nodeForSecond.canAssign(slotPair.getSecond())) {
								nodeForSecond.assign(slotPair.getSecond());
							} else {
								logger.debug("Cannot assign slot " + slotPair.getSecond() + " to node " + nodeForSecond);
								assignmentOK = false;
							}
							
							if (assignmentOK) {
								int tmpInterNodeTraffic = TrafficManager.getInstance().computeInterNodeTraffic();
								logger.debug("...the inter-node traffic is " + tmpInterNodeTraffic + " tuple/s");
								if (minInterNodeTraffic == -1 || tmpInterNodeTraffic < minInterNodeTraffic) {
									bestNodeForFirst = nodeForFirst;
									bestNodeForSecond = nodeForSecond;
									minInterNodeTraffic = tmpInterNodeTraffic;
								}
							}
							
							if (nodeForFirst.contains(slotPair.getFirst()))
								nodeForFirst.remove(slotPair.getFirst());
							if (nodeForSecond.contains(slotPair.getSecond()))
								nodeForSecond.remove(slotPair.getSecond());
						}
					}
					
					logger.debug("The best assignment is slot " + slotPair.getFirst() + " to node " + bestNodeForFirst + " and slot " + slotPair.getSecond() + " to node " + bestNodeForSecond + ", with inter-node traffic " + minInterNodeTraffic + " tuple/s");
					bestNodeForFirst.assign(slotPair.getFirst());
					bestNodeForSecond.assign(slotPair.getSecond());
					logger.info("Slot " + slotPair.getFirst() + " assigned to node " + bestNodeForFirst + " (Slots of topology " + slotPair.getFirst().getTopology().getTopologyID() + " in this node: " + bestNodeForFirst.getTopologySlotCount(slotPair.getFirst().getTopology().getTopologyID()));
					logger.info("Slot " + slotPair.getSecond() + " assigned to node " + bestNodeForSecond + " (Slots of topology " + slotPair.getSecond().getTopology().getTopologyID() + " in this node: " + bestNodeForSecond.getTopologySlotCount(slotPair.getSecond().getTopology().getTopologyID()));
					
				} /* end if (!nodeList.isEmpty()) */
				
				logger.info("Assignment of slots " + slotPair + " completed");
				
			} /* end for (SlotPair slotPair : interSlotTrafficList) */
			
			if (TrafficManager.getInstance().getAssignments() != null)
				logger.info("Intermediate assignment: " + Utils.collectionToString(TrafficManager.getInstance().getAssignments().keySet()));
			
			/*
			 *  ensure that the slots of a given topology are assigned to the proper number of nodes,
			 *  otherwise the chances of parallelization/pipelining are not rightly exploited
			 */
			logger.info("Check whether all the topologies are using the desired number of nodes");
			for (Topology topology : topologyList) {
				// int numberOfNodesToUse = Math.min(topology.getSlots().size(), nodeManager.getNodeCount());
				int numberOfNodesToUse = topology.getNumberOfNodesToUse(nodeManager.getNodeCount());
				List<Node> usedNodeList = null;
				while (	TrafficManager.getInstance().getNodeList(topology) != null &&
						(usedNodeList = new ArrayList<Node>( TrafficManager.getInstance().getNodeList(topology) )).size() < numberOfNodesToUse)
				{
					logger.info("Topology " + topology + " is using " + usedNodeList.size() + " nodes, while it should use " + numberOfNodesToUse);
					Node bestUsedNode = null;
					Node bestUnusedNode = null;
					Slot bestSlot = null;
					int bestTraffic = -1;
					for (Node usedNode : usedNodeList) {
						// check if this node has more than one slot for that topology
						int topologySlotCount = 0;
						List<Slot> nodeSlotList = new ArrayList<Slot>( usedNode.getSlotList() );
						for (Slot slot : nodeSlotList)
							if (slot.getTopology() == topology)
								topologySlotCount++;
						if (topologySlotCount > 1) {
							for (Slot slot : nodeSlotList) {
								Node unusedNode = nodeManager.getUnusedNode(usedNodeList, slot);
								if (unusedNode != null) {
									usedNode.remove(slot);
									unusedNode.assign(slot);
									int traffic = TrafficManager.getInstance().computeInterNodeTraffic();
									logger.info("Moving slot " + slot + " from node " + usedNode + " to node " + unusedNode + ", the traffic becomes " + traffic + " tuple/s");
									if (bestUsedNode == null || traffic < bestTraffic) {
										bestUsedNode = usedNode;
										bestUnusedNode = unusedNode;
										bestSlot = slot;
										bestTraffic = traffic;
									}
									unusedNode.remove(slot);
									usedNode.assign(slot);
								} /* end if (unusedNode != null) */
							} /* end for (Slot slot : nodeSlotList) */
						} /* end if (topologySlotCount > 1) */
					} /* end for (Node usedNode : usedNodeList) */
					
					if (bestUnusedNode != null) {
						logger.info("The best is moving slot " + bestSlot + " from node " + bestUsedNode + " to node " + bestUnusedNode + ", with a traffic of " + bestTraffic + " tuple/s");
						bestUsedNode.remove(bestSlot);
						bestUnusedNode.assign(bestSlot);
					} else {
						logger.info("Cannot find a way to make topology " + topology + " use the desired number of nodes");
						break;
					}
					
				} /* end while (number of used nodes < number of nodes to use */
			} /* end for (Topology topology : topologyList) */
		}
		
		logger.info("Second phase completed!");
		if (TrafficManager.getInstance().getAssignments() != null)
			logger.info("Final assignment: " + Utils.collectionToString(TrafficManager.getInstance().getAssignments().keySet()));
	}

}
