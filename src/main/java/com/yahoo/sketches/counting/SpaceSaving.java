package com.yahoo.sketches.counting;

import java.util.PriorityQueue;
import java.util.HashMap;

//@SuppressWarnings("cast")
public class SpaceSaving {
  
  private PriorityQueue<Pair> queue; 
  private HashMap<Long,Integer> counts;
  //size is number of counters
  private int size;
  //updates tracks current stream length
  private int updates;
      
  public SpaceSaving(int size) {	
	  //queue is used for quick access to counter with minimal count
	  this.queue = new PriorityQueue<Pair>(size);
	  //stores a hash table of counters, to allow updating an item's count if it tracked
	  this.counts = new HashMap<Long,Integer>(size);
	  this.size = size;
	  this.updates = 0;
  }
	
  public void update(long key, int increment) {
	  //if key is already assigned a counter
	if(counts.containsKey(key)){
		int old_count = counts.get(key);
		int new_count = old_count + increment;
		//update count of key in hash table
		counts.put(key, new_count);

		//update count of key in min-heap by 
		//removing it and adding it back in with new count
		queue.remove(new Pair(key, 0));
		queue.add(new Pair(key, new_count));
	}
	else{//if key not already assigned a counter, 
		//and not all counters are used, assign it one
		if(counts.size() < size){
			counts.put(key, 1);
			queue.add(new Pair(key, increment));
		}
		else{
			//if all counters are used, assign the smallest counter to the key
			Pair lowest = queue.peek();
			int new_priority = lowest.getvalue() + increment;
			counts.remove(lowest.getname());
			counts.put(key, new_priority);
			queue.remove(lowest);
			queue.add(new Pair(key, new_priority));
		}
	}
	//register the updated stream length
	this.updates = this.updates + increment;
	System.out.printf("At end of update, queue.size() is: %d and counts.size() is: %d%n", queue.size(), counts.size());
  }
  
  public double getEstimate(long key) { 
	  //return count of associated counter if key is tracked
	  //else, return 0 if key not tracked, and not all counters used
	  //else, return minimum counter value
	  //note this estimate is always an upper bound on the true freq of key
	  if(counts.containsKey(key))
	  {
		  return counts.get(key);
	  }
	  else{
		  if(queue.size()< this.size){
			  return 0;
		  }
		  else{
			  //System.out.printf("queue.peek().name is: %d%n", queue.peek().name);
			  return queue.peek().getvalue();
		  }
	  }
  }

  public double getUpperBound(long key) {
    return getEstimate(key);
  }

  public double getLowerBound(long key) {
	// return upper bound on key's freq, minus upper bound on error
	if(this.updates < this.size){
		return getEstimate(key);
	}
	else{
		return getEstimate(key) - 1.0 * this.updates/this.size;
	}
  }
}

