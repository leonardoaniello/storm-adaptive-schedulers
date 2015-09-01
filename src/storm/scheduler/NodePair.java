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

public class NodePair {

	private final Node first, second;
	private int traffic;
	
	public NodePair(Node first, Node second) {
		this.first = first;
		this.second = second;
	}

	public Node getFirst() {
		return first;
	}

	public Node getSecond() {
		return second;
	}

	public int getTraffic() {
		return traffic;
	}
	
	public void addTraffic(int traffic) {
		this.traffic += traffic;
	}
	
	public void removeTraffic(int traffic) {
		if (this.traffic - traffic >= 0)
			this.traffic -= traffic;
		else
			throw new RuntimeException("Cannot remove traffic for node pair " + this);
	}
	
	@Override
	public String toString() {
		return "{" + first + ", " + second + "}";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
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
		NodePair other = (NodePair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
}
