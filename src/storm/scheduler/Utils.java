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
import java.util.List;

public class Utils {
	
	// public static final int ACKER_TAKS_ID = 1;
	public static final String ALFA = "alfa"; // between 0 and 1
	public static final String BETA = "beta"; // between 0 and 1
	public static final String GAMMA = "gamma"; // greater than 1
	public static final String DELTA = "delta"; // between 0 and 1
	public static final String TRAFFIC_IMPROVEMENT = "traffic.improvement"; // between 1 and 100
	public static final String RESCHEDULE_TIMEOUT = "reschedule.timeout"; // in s

	private Utils() {}
	
	/**
	 * @param task
	 * @param executorList
	 * @return the executor where the given task lives in
	 */
	public static Executor getExecutor(int task, List<Executor> executorList) {
		for (Executor executor : executorList)
			if (task >= executor.getBeginTask() && task <= executor.getEndTask())
				return executor;
		/*if (task == ACKER_TAKS_ID) {
			Executor ackerExecutor = new Executor(ACKER_TAKS_ID, ACKER_TAKS_ID);
			ackerExecutor.setTopologyID(executorList.get(0).getTopologyID());
			return ackerExecutor;
		}*/
		return null;
	}
	
	/**
	 * @param list
	 * @return the list in csv format
	 */
	public static String collectionToString(Collection<?> list) {
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
	
}
