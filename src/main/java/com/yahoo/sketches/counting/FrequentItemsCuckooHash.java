package com.yahoo.sketches.counting;

public class FrequentItemsCuckooHash {
  CuckooHashWithImplicitDeletions counters;
  long error;
  
  /**
   * @param maxSize (must be positive)
   * Gives the maximal number of positive counters the sketch is allowed to keep.
   * This should be thought of as the limit on its space usage. The size is dynamic.
   * If fewer than maxSize different keys are inserted the size will be smaller 
   * that maxSize and the counts will be exact.  
   */
  public FrequentItemsCuckooHash(int maxSize) {
    if (maxSize <= 0) throw new IllegalArgumentException("Received negative or zero value for maxSize.");
    counters = new CuckooHashWithImplicitDeletions(maxSize);
    error=0;
  }
 
  /**
   * @param key whose count estimate is returned.
   * @return the approximate count for the key.
   * It is guaranteed that
   * 1) get(key) <= real count
   * 2) get(key) >= real count - getMaxError() 
   */
  public long get(long key) {
    return counters.get(key);
  }

  /**
   * @return the maximal error of the estimate one gets from get(key).
   * Note that the error is one sided. if the real count is realCount(key) then
   * get(key) <= realCount(key) <= get(key) + getMaxError() 
   */
  public long getMaxError() {
    return error;
  }

  
  /**
   * @param key 
   * A key (as long) to be added to the sketch. The key cannot be null.
   */
  public void update(long key) {
    if (!counters.increment(key)) {
      counters.decrement();
      error++;
    }
  }

  /**
   * @param that
   * Another FrequentItems sketch. Potentially of different size. 
   * @return pointer to the sketch resulting in adding the approximate counts of another sketch. 
   * This method does not create a new sketch. The sketch whose function is executed is changed.
   */
  public FrequentItemsCuckooHash union(FrequentItemsCuckooHash that) {
    // Stub function 
    return this;
  }
}
