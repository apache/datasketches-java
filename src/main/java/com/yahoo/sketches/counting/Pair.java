package com.yahoo.sketches.counting;

public class Pair implements Comparable<Pair> {
	private long name;
	private Integer value;
	public Pair(long name, int value) {
		this.name = name;
		this.value = value;

	}
	public long getname(){
		return name;
	}    
	public int getvalue() {
		return value;
	}

	public int compare(Pair o1, Pair o2) {
		Pair a1 = (Pair)o1;
		Pair a2 = (Pair)o2;
		if(a1.value>a2.value) {
			return 1;
		}
		else if(a1.value<a2.value) {
			return -1;
		}
		return 0;

	}
	
	public int compareTo(Pair o2) {
		Pair a2 = (Pair)o2;
		if(this.value>a2.value) {
			return 1;
		}
		else if(this.value<a2.value) {
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