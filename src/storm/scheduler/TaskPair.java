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

public class TaskPair {
	
	// private static final int MAX_TASK_ID = 1000000000;

	private final int sourceTaskId;
	private final int destinationTaskId;
	private final String toString;
	// private final int hashCode;
	
	public TaskPair(int sourceTaskId, int destinationTaskId) {
		this.sourceTaskId = sourceTaskId;
		this.destinationTaskId = destinationTaskId;
		toString = "[" + sourceTaskId + "->" + destinationTaskId + "]";
		// hashCode = sourceTaskId * MAX_TASK_ID + destinationTaskId;
	}

	public int getSourceTaskId() {
		return sourceTaskId;
	}

	public int getDestinationTaskId() {
		return destinationTaskId;
	}
	
	@Override
	public String toString() {
		return toString;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + destinationTaskId;
		result = prime * result + sourceTaskId;
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
		TaskPair other = (TaskPair) obj;
		if (destinationTaskId != other.destinationTaskId)
			return false;
		if (sourceTaskId != other.sourceTaskId)
			return false;
		return true;
	}
	
}
