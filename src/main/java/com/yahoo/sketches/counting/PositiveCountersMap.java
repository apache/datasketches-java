package com.yahoo.sketches.counting;

import java.util.HashMap;
import java.util.Map.Entry;

public class PositiveCountersMap{
	
	private HashMap<Long,Long> counters;
	private long offset;
	
	public PositiveCountersMap(){
		counters = new HashMap<Long,Long>();
		offset = 0L;
	}
	
	public int size(){
		return counters.size();
	}
	
	// This function is also used to purge zero or negative 
	// counters when encountered
	public Long get(Long key){
		assert(key != null);
		Long value = counters.get(key);
		if (value == null){
			return 0L; 
		} else if (value <= offset) {
			counters.remove(key);
			return 0L;
		} else {
			return value - offset;
		}
	}
	
	public void put(Long key, Long value){
		assert(key != null);
		assert(value != null);
		assert(value > 0);
		counters.put(key, get(key) + value + offset); 
	}
	
	public void increment(Long key, long delta){
		counters.put(key, get(key) + delta + offset);
	}
	
	public void increment(Long key){
		counters.put(key, get(key) + 1 + offset);
	}
	
	public void decerementAll(Long delta){
		offset += delta;
		for (Entry<Long, Long> entry : counters.entrySet()) {
      if (entry.getValue() <= offset){
        counters.remove(entry.getKey());
      }
		}
	}
}
