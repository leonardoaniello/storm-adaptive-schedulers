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

/**
 * models a pair of slots and its inter-slot traffic
 * @author Leonardo
 *
 */
public class SlotPair {

	private final Slot first, second;
	private int traffic;
	
	public static long getKey(Slot first, Slot second) {
		return first.hashCode() << 32 + second.hashCode();
	}
	
	public SlotPair(Slot first, Slot second) {
		this.first = first;
		this.second = second;
	}

	public int getTraffic() {
		return traffic;
	}

	public void addTraffic(int traffic) {
		this.traffic += traffic;
	}
	
	public void removeTraffic(int traffic) {
		this.traffic -= traffic;
		// sanity check
		if (this.traffic < 0)
			throw new RuntimeException("Traffic lower than zero...(traffic: " + this.traffic + ", removed traffic: " + traffic + ")");
	}

	public Slot getFirst() {
		return first;
	}

	public Slot getSecond() {
		return second;
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
		SlotPair other = (SlotPair) obj;
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
	
	@Override
	public String toString() {
		return "{" + first + "; " + second + "; traffic: " + traffic + " tuple/s}";
	}
}
