package com.yahoo.sketches.counting;

public class Pair implements Comparable<Pair> {
	private long name;
	private long value;
	public Pair(long name, long value) {
		this.name = name;
		this.value = value;

	}
	public long getname(){
		return name;
	}    
	public long getvalue() {
		return value;
	}

	public int compare(Pair o1, Pair o2) {
		if(o1.value>o2.value) {
			return 1;
		}
		else if(o1.value<o2.value) {
			return -1;
		}
		return 0;

	}
	
	@Override 
	public int compareTo(Pair o2) {
		if(this.value>o2.value) {
			return 1;
		}
		else if(this.value<o2.value) {
			return -1;
		}
		return 0;

	}

	@Override
	public int hashCode() {
		int hash = 3;
		return hash;
	}
	@Override
	public boolean equals(Object o) {
		Pair a2 = (Pair)o;
		return this.name == a2.name;
	}
}