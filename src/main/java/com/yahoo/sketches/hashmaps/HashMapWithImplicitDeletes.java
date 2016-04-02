package com.yahoo.sketches.hashmaps;


public class HashMapWithImplicitDeletes extends HashMap {

  private final short AVAILABLE_STATE = 0;
  private final short OCCUPIED_STATE = 1;
  private final short DELETED_STATE = 2;
  private int maximalDrift = 0;

  public HashMapWithImplicitDeletes(int capacity) {
    super(capacity);
  }

  @Override
  public boolean isActive(int probe) {
    return (states[probe] == 1);
  }

  @Override
  public long get(long key) {
    int probe = hashProbe(key);
    return (keys[probe] == key && states[probe] == OCCUPIED_STATE) ? values[probe] : 0;
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    int probe = hashProbe(key);
    if (states[probe] != OCCUPIED_STATE) {
      assert (size < capacity);
      keys[probe] = key;
      values[probe] = putAmount;
      states[probe] = OCCUPIED_STATE;
      size++;
    } else {
      assert (keys[probe] == key);
      values[probe] += adjustAmount;
    }
  }

  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    for (int i = 0; i < length; i++) {
      if (states[i] == OCCUPIED_STATE && values[i] <= thresholdValue) {
        states[i] = DELETED_STATE;
        size--;
      }
    }
  }

  private int hashProbe(long key) {
    int hash = (int) hash(key) & arrayMask;
    int drift = 0;
    while (states[hash] == OCCUPIED_STATE && keys[hash] != key) {
      hash = (hash + 1) & arrayMask;
      drift++;
    }

    // found either the key or a free spot, return that.
    if (keys[hash] == key || states[hash] == AVAILABLE_STATE) {
      if (drift > maximalDrift)
        maximalDrift = drift;
      return hash;
    }

    // found a deleted spot, need to return this if key is not in the map
    assert (states[hash] == DELETED_STATE);
    int firstDeletedHash = hash;

    // looking for the key
    while (states[hash] != AVAILABLE_STATE && keys[hash] != key && drift++ <= maximalDrift)
      hash = (hash + 1) & arrayMask;
    // if the key is found, return the key,
    // otherwise, return the first deleted position for insertion.
    return (keys[hash] == key && states[hash] == OCCUPIED_STATE) ? hash : firstDeletedHash;
  }

}
