package com.yahoo.sketches.hashmaps;

public class HashMapLinearProbingWithRebuilds extends HashMap {

  public HashMapLinearProbingWithRebuilds(int capacity) {
    super(capacity);
  }

  @Override
  public boolean isActive(int probe) {
    return (states[probe] > 0);
  }

  @Override
  public long get(long key) {
    int probe = hashProbe(key);
    return (states[probe] > 0) ? values[probe] : 0;
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    int probe = hashProbe(key);
    if (states[probe] == 0) {
      keys[probe] = key;
      values[probe] = putAmount;
      states[probe] = 1;
      size++;
    } else {
      assert (keys[probe] == key);
      values[probe] += adjustAmount;
    }
  }

  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    HashMapLinearProbingWithRebuilds rebuiltHashMap =
        new HashMapLinearProbingWithRebuilds(capacity);
    for (int i = 0; i < length; i++)
      if (states[i] > 0 && values[i] > thresholdValue) {
        rebuiltHashMap.adjustOrPutValue(keys[i], values[i], values[i]);
      }
    System.arraycopy(rebuiltHashMap.keys, 0, keys, 0, length);
    System.arraycopy(rebuiltHashMap.values, 0, values, 0, length);
    System.arraycopy(rebuiltHashMap.states, 0, states, 0, length);
    size = rebuiltHashMap.getSize();
  }

  /**
   * @param key to search for in the array
   * @return returns the location of the key in the array or the first possible place to insert it.
   */
  private int hashProbe(long key) {
    long hash = hash(key);
    int probe = (int) (hash) & arrayMask;
    while (keys[probe] != key && states[probe] != 0)
      probe = (probe + 1) & arrayMask;
    return probe;
  }

}
