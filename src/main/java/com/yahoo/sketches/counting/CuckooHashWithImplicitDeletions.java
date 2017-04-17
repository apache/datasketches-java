package com.yahoo.sketches.counting;

public class CuckooHashWithImplicitDeletions {  
  private long offset;
  final private double LOAD_FACTOR = 0.5;
  final private int LOCATIONS_PER_KEY = 10;
  
  private int keyValueArrayLength;
  private long[] keys;
  private long[] values;
  public int successes = 0;
  final private int bigPrime = 2147483647;
  
  public CuckooHashWithImplicitDeletions(int maxSize) {  
    if (maxSize <= 0) throw new IllegalArgumentException("Received negative or zero value for maxSize.");
    keyValueArrayLength = (int) (maxSize/LOAD_FACTOR);
    keys = new long[keyValueArrayLength];
    values = new long[keyValueArrayLength];
    offset = 0;  
  }
  
  private int getArrayIndex(long key, int index){
    int hash = (((int) key+index)*bigPrime) % keyValueArrayLength;
    return (hash >=0)?hash:-hash;
  }
  
  public long get(long key) {
    for (int i=LOCATIONS_PER_KEY; i-- >0;){
      int hash = getArrayIndex(key,i);
      if (keys[hash] == key) {
        long value = values[hash];
        return (value > offset) ? value-offset : 0;
      }
    }
    return 0;
  }
      
  public boolean increment(long key) {
    // In case the key is in the map already
    int availableIndex = -1;
    for (int i=LOCATIONS_PER_KEY; i-- >0;){
      int hash = getArrayIndex(key,i);
      if (keys[hash] == key) {
        long value = values[hash];
        values[hash] = (value > offset) ? value+1: offset+1;
        return true;
      }
      if (availableIndex < 0 && values[hash] <= offset) {
        availableIndex = hash;
      }
    }
    // The key is not in the map but there is a spot for it
    if (availableIndex >= 0) {
      keys[availableIndex] = key;
      values[availableIndex] = offset+1;
      return true;
    }
    // Need to add bump(key)
    return false;
  }
  
  public void decrement(){
    offset++;
  }
  
}
