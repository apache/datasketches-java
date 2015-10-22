package com.yahoo.sketches.counting;

import java.util.HashMap;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
//import java.util.List;

import com.yahoo.sketches.Util;
import static com.yahoo.sketches.hash.MurmurHash3.hash;


@SuppressWarnings("cast")
public class FrequentItems {
  
  private HashMap<Long,Integer> counts;
  private int maxSize;
  private ArrayList<Long> keysToDelete;
  private int maxDeletesPerKey;
  
  public FrequentItems(int maxSize) {
	  this.maxSize = maxSize;
	  this.counts = new HashMap<Long,Integer>(maxSize);
	  this.keysToDelete = new ArrayList<Long>();
	  this.maxDeletesPerKey = 0;
  }
	
  
  
  public void update(int key) {
    updateWithHash(hash(new int[] {key}, Util.DEFAULT_UPDATE_SEED));
  }
  
  
  public Integer getEstimate(long[] key) {
  	return getEstimateWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  

  public Integer getUpperBound(int key) {
  	return getUpperBoundWithHash(hash(new int[] {key}, Util.DEFAULT_UPDATE_SEED));
  }

  public FrequentItems union(FrequentItems that) {
  	// Summing up the counts
  	for (Entry<Long, Integer> entry : that.counts.entrySet()) {
  		Long key = entry.getKey();
  		int estimate = (Integer) this.counts.get(key);
  		this.counts.put(key, estimate + entry.getValue());
  	}
  	// The count for a specific key could have been
  	// deleted the maximal number of times in both sketches
  	this.maxDeletesPerKey += that.maxDeletesPerKey;
  	
  	if (counts.size() >= maxSize){ 
	  	// Crude way to find mind the value of the maxSize'th element in the array
	  	ArrayList<Integer> valuesArray = new ArrayList<Integer>(that.counts.values());
	  	Collections.sort(valuesArray);
	  	int theresholdValue = valuesArray.get(maxSize-1);
	  	
	  	// Decrementing all counts by theresholdValue  
	  	for (Entry<Long, Integer> entry : counts.entrySet()) {
		    if (entry.getValue() <= theresholdValue){
		    	keysToDelete.add(entry.getKey());
		    } else {
		    	counts.put(entry.getKey(), entry.getValue() - theresholdValue);
		    }  
			}
	  	maxDeletesPerKey += theresholdValue;
	  	
	  	// Deleting counts that are below the threshold 
			for (Long tempHash : keysToDelete) {
				counts.remove(tempHash);
			}
			keysToDelete.clear();
	  }
		// returning a pointer to self
  	return this;
  }
  
  
  public void update(long[] key) {
    updateWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public void update(int[] key) {
    updateWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public Integer getEstimate(int key) {
  	return getEstimateWithHash(hash(new int[] {key}, Util.DEFAULT_UPDATE_SEED));
  }
  
  public Integer getEstimate(int[] key) {
  	return getEstimateWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public Integer getUpperBound(long[] key) {
  	return getUpperBoundWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public Integer getUpperBound(int[] key) {
  	return getUpperBoundWithHash(hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public int getSize() {
	  return counts.size();
  }
  
  public int getDeletePhases() {
  	return maxDeletesPerKey;
  }
  
  private void updateWithHash(long[] hash) {
  	Long half = hash[0];
  	if(counts.containsKey(half)){
			counts.put(half, counts.get(half)+1);
		} else {
			counts.put(half,1);
		}
			
		// In case the hash map is full we delete a few entries. 
  	// It is guaranteed that at least one entry will be deleted.
		if (counts.size() >= maxSize) {
			for (Entry<Long, Integer> entry : counts.entrySet()) {
		    if (entry.getValue() <= 1){
		    	keysToDelete.add(entry.getKey());
		    } else {
		    	counts.put(entry.getKey(), entry.getValue()-1);
		    }  
			}
			
			maxDeletesPerKey += 1; 
			for (Long tempHash : keysToDelete) {
				counts.remove(tempHash);
			}
			keysToDelete.clear();
		}
  }
  
  private Integer getEstimateWithHash(long[] hash) {
  	Long half = hash[0];
  	return (counts.containsKey(half)) ? counts.get(half) : 0;
  }

  private Integer getUpperBoundWithHash(long[] hash) {
  	int lowerBound = getEstimateWithHash(hash) + maxDeletesPerKey;
  	return (lowerBound > 0) ? lowerBound : 0;
  }
  
  
}
