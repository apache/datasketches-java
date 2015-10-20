package com.yahoo.sketches.counting;

import java.util.HashMap;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.hash.MurmurHash3;


//@SuppressWarnings("cast")
public class FrequentItems {
  
  private HashMap<long[],Integer> counts;
  
  public FrequentItems(int size) {	  
	  this.counts = new HashMap<long[],Integer>(size);
  }
	
  public void update(long[] key) {
    updateWithHash(MurmurHash3.hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  public void update(int[] key) {
    updateWithHash(MurmurHash3.hash(key, Util.DEFAULT_UPDATE_SEED));
  }
  
  private void updateWithHash(long[] hash) {
	if(counts.containsKey(hash)){
		counts.put(hash, counts.get(hash)+1);
	}
	//This is where the update function should do something
  }
  
  public double getEstimate(int[] key) { 
	// return approx count + half max error 
	  return 0.0;
  }

  public double getUpperBound(int[] key) {
    // return approx count + max error
    return 0.0;
  }

  public double getLowerBound(int[] key) {
	// return approx count
    return 0.0;
  }

}
