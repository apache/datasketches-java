package com.yahoo.sketches.counting;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.ArrayList;

/**
 * This is a utility class that implements (and abstracts) a set of positive counters. 
 * The mapping is  Long key -> Long count.
 * The default value of any key is zero and no negative counters are allowed. 
 * Non-positive mappings are deleted. 
 * 
 * It also support incrementing individual counters and decrementing all counters simultaneously.
 * This is a convenient and efficient modification intended to be used in FrequentDirection sketching.
 * 
 * @author edo 
 */
public class PositiveCountersMap{
  
  private HashMap<Long,Long> counters;
  ArrayList<Long> keysToRemove;
  private long offset;
  private long nnz;

  /**
   * Creates empty mappings and default offset = 0.
   */
  public PositiveCountersMap(){
    counters = new HashMap<Long,Long>();
    keysToRemove = new ArrayList<Long>();
    offset = 0L;
  }
  
  /**
   * @return the number of positive counters
   */
  public long nnz(){
    return nnz;
  }
  
  /**
   * @return an iterator over the positive count values 
   */
  public Collection<Long> values(){
    return counters.values();
  }
  
  /**
   * @return an iterator over the keys corresponding to positive counts only
   */
  public Collection<Long> keys(){
    return counters.keySet();
  }
  
  /**
   * @param key, should not be null.
   * @return the exact count for that key.
   */
  public Long get(Long key){
    assert(key != null);
    Long value = counters.get(key);
    return (value != null) ? value - offset: 0L;
  }
  
  /**
   * @param key whose count needs to be set to a differnt value
   * @param value of new count for the key
   */
  public void put(Long key, Long value){
    assert(key != null);
    assert(value != null);
    assert(value > 0);
    counters.put(key, get(key) + value + offset); 
  }
  
  /**
   * @param key whose count should be incremented. If a counter
   * does not exist for key it is created.
   * @param delta, the amount by which the value should be increased.
   * The variable delta must be positive.
   */
  public void increment(Long key, long delta){
    assert(delta > 0);
    if (delta <= 0) return;
    long value =  get(key);
    if (value == 0) nnz++;
    counters.put(key, value + delta + offset);
  }
  
  /**
   * @param key whose count should be incremented by 1. 
   * If a counter does not exist for key it is created.
   */
  public void increment(Long key){
    increment(key, 1L);
  }
  
  /**
   * @param that, another PositiveCountersMap
   * All counters of shared keys are summed up.
   * Keys only in the other PositiveCountersMap receive new counts. 
   */
  public void increment(PositiveCountersMap that){
    for (Entry<Long, Long> entry : that.counters.entrySet()) {
      increment(entry.getKey(), entry.getValue());
    }
    removeNegativeCounters();
    nnz = counters.size();
  }
  
  /**
   * @param delta, the value by which all counts should be decremented.
   * delta must be positive.
   */
  public void decerementAll(Long delta){
    assert(delta > 0);
    if (delta <= 0) return;
    offset += delta;
    removeNegativeCounters();
    nnz = counters.size();
  }
  
  /**
   * decreases all counts by 1.
   */
  public void decerementAll(){
    decerementAll(1L);
  }
  
  /**
   * This is an internal function that cleans up non-positive counts and frees up space.
   */
  private void removeNegativeCounters(){
    for (Entry<Long, Long> entry : counters.entrySet()) {
      if (entry.getValue() <= offset) {
        keysToRemove.add(entry.getKey());
      }
    }
    for (Long key : keysToRemove) {
      counters.remove(key);
    }
    keysToRemove.clear();
  }
  
}
