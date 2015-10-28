package com.yahoo.sketches.counting;

import java.util.ArrayList;
import java.util.Collections;

/**
 * This class implements a simple frequent direction algorithm.
 * It is not intended to be very fast but rather to provide a 
 * correctness baseline for faster versions 
 * 
 * The algorithm is most commonly known as the "Misra-Gries algorithm", 
 * "frequent items" or "space-saving". It was discovered and rediscovered and redesigned several times over the years.
 * "Finding repeated elements", Misra, Gries, 1982 
 * "Frequency estimation of internet packet streams with limited space" Demaine, Lopez-Ortiz, Munro, 2002
 * "A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker, Papadimitriou, 2003
 * "Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally, Agrawal, Abbadi, 2006
 * 
 * @author edo
 */
public class FrequentItems {

  private PositiveCountersMap counters;
  private int maxSize;
  private int maxError;
  
  /**
   * @param maxSize (must be positive)
   * Gives the maximal number of positive counters the sketch is allowed to keep.
   * This should be thought of as the limit on its space usage. The size is dynamic.
   * If fewer than maxSize different keys are inserted the size will be smaller 
   * that maxSize and the counts will be exact.  
   */
  public FrequentItems(int maxSize) {
    assert(maxSize > 0);
    this.maxSize = maxSize;
    counters = new PositiveCountersMap();
    this.maxError = 0;
  }
  
  /**
   * @return the number of positive counters in the sketch.
   */
  public long nnz() {
    return counters.nnz();
  }
  
  /**
   * @param key (should be "null") whose frequency (number of insertions) is needed.
   * @return the approximate count for the number of times the 
   * key was inserted into the sketch (using update(key))
   * 
   */
  public long get(Long key) {
    assert(key != null);
    return counters.get(key);
  }

  /**
   * @return the maximal error of the estimate one gets from get(key).
   * Note that the error is one sided. if the real count is realCount(key) then
   * get(key) <= realCount(key) <= get(key) + getMaxError() 
   */
  public int getMaxError() {
    return maxError;
  }

  
  /**
   * @param key 
   * A key (as long) to be added to the sketch. The key cannot be null.
   */
  public void update(Long key) {
    assert(key != null);
    counters.increment(key);  
    if (counters.nnz() > maxSize){ 
      counters.decerementAll();
      maxError++;
    }
  }

  /**
   * @param that
   * Another FrequentItems sketch. Potentially of different size. 
   * @return pointer to the sketch resulting in adding the approximate counts of another sketch. 
   * This method does not create a new sketch. The sketch whose function is executed is changed.
   */
  public FrequentItems union(FrequentItems that) {
    // Summing up the counts
    counters.increment(that.counters);
    
    // The count for a specific key could have been
    // deleted the maximal number of times in both sketches
    this.maxError += that.maxError;
    
    if (counters.nnz() > maxSize){ 
      // Crude way to find mind the value of the 
      // maxSize'th largest element in the array  
      ArrayList<Long> valuesArray = new ArrayList<Long>(counters.values());
      Collections.sort(valuesArray);
      long delta = valuesArray.get(maxSize);
      counters.decerementAll(delta);
      maxError += delta;
    }
    return this;
  }

}
