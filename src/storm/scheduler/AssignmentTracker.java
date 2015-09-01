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

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;

public class AssignmentTracker {

	/**
	 * host -> (slot port -> list of executors)
	 */
	private Map<String, Map<Integer, List<String>>> lastAssignment;
	
	private Logger logger = Logger.getLogger(AssignmentTracker.class);
	
	public void checkAssignment(Topologies topologies, Cluster cluster) {
		try {
			// compile current assignment
			List<String> topologyList = new ArrayList<String>();
			Map<String, Map<Integer, List<String>>> assignment = new HashMap<String, Map<Integer,List<String>>>();
			for (String topologyId : cluster.getAssignments().keySet()) {
				topologyList.add(topologyId);
				SchedulerAssignment topologyAssignment = cluster.getAssignmentById(topologyId);
				Map<ExecutorDetails, WorkerSlot> realAssignment = topologyAssignment.getExecutorToSlot();
				for (ExecutorDetails executor : realAssignment.keySet()) {
					// get executor info
					WorkerSlot slot = realAssignment.get(executor);
					String host = cluster.getSupervisorById( slot.getNodeId() ).getHost();
					int port = slot.getPort();
					String executorDescription = 
						topologies.getById(topologyId).getExecutorToComponent().get(executor) + 
						"[" + executor.getStartTask() + "," + executor.getEndTask() + "]";
					
					// put into assignment
					Map<Integer, List<String>> hostAssignment = assignment.get(host);
					if (hostAssignment == null) {
						hostAssignment = new HashMap<Integer, List<String>>();
						assignment.put(host, hostAssignment);
					}
					List<String> executorsForSlot = hostAssignment.get(port);
					if (executorsForSlot == null) {
						executorsForSlot = new ArrayList<String>();
						hostAssignment.put(port, executorsForSlot);
					}
					executorsForSlot.add(executorDescription);
				}
			}
			
			if (lastAssignment == null || !assignmentsAreEqual(assignment, lastAssignment)) {
				lastAssignment = assignment;
				if (!lastAssignment.keySet().isEmpty()) {
					String serializedAssignment = serializeAssignment(lastAssignment);
					logger.info("ASSIGNMENT CHANGED");
					logger.info(serializedAssignment);
					try {
						DataManager.getInstance().StoreAssignment(Utils.collectionToString(topologyList), serializedAssignment);
					} catch (Exception e) {
						logger.error("An error occurred storing an assignment", e);
					}
				}
			}
		} catch (Exception e) {
			logger.error("An error occurred tracking an assignment", e);
		}
	}
	
	private String serializeAssignment(Map<String, Map<Integer, List<String>>> assignment) {
		StringBuffer sb = new StringBuffer();
		for (String host : assignment.keySet()) {
			Map<Integer, List<String>> hostAssignment = assignment.get(host);
			for (int port : hostAssignment.keySet()) {
				String executors = Utils.collectionToString(hostAssignment.get(port));
				sb.append("(" + host + ":" + port + " " + executors + ")");
			}
		}
		return sb.toString();
	}
	
	private boolean assignmentsAreEqual(Map<String, Map<Integer, List<String>>> a1, Map<String, Map<Integer, List<String>>> a2) {
		if (a1.keySet().size() != a2.keySet().size())
			return false;
		
		for (String topologyId : a1.keySet()) {
			if (!a2.keySet().contains(topologyId))
				return false;
			
			Map<Integer, List<String>> t1 = a1.get(topologyId);
			Map<Integer, List<String>> t2 = a2.get(topologyId);
			if (t1.keySet().size() != t2.keySet().size())
				return false;
			
			for (int port : t1.keySet()) {
				if (!t2.keySet().contains(port))
					return false;
				
				List<String> e1 = t1.get(port);
				List<String> e2 = t2.get(port);
				if (e1.size() != e2.size())
					return false;
				
				for (String executor : e1)
					if (!e2.contains(executor))
						return false;
			}
		}
		
		return true;
	}
}
