/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hashmaps;

public class HashMapRobinHood extends HashMap {
  int MAX_STATE_ALLOWED = 50;

  public HashMapRobinHood(int capacity) {
    super(capacity);
  }

  @Override
  public boolean isActive(int probe) {
    return (states[probe] > 0);
  }

  @Override
  public long get(long key) {
    int probe = (int) hash(key) & arrayMask;
    while (states[probe] >= 0 && keys[probe] != key)
      probe = (probe + 1) & arrayMask;
    return (keys[probe] == key && states[probe] > 0) ? values[probe] : 0;
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    int probe = (int) hash(key) & arrayMask;
    byte state = 1;
    while (states[probe] >= state && keys[probe] != key) {
      state++;
      probe = (probe + 1) & arrayMask;
    }

    // found the key
    if (keys[probe] == key && states[probe] > 0) {
      values[probe] += adjustAmount;
      return;
    }

    // found a vacant spot
    if (states[probe] == 0) {
      assert (size < capacity);
      keys[probe] = key;
      values[probe] = putAmount;
      states[probe] = state;
      size++;
      return;
    }

    // copy the items further in the sequence one spot to the right.
    // and insert the item at the probe location
    assert (size < capacity);
    int rightProbe = (probe + 1) & arrayMask;
    while (states[rightProbe] > 0)
      rightProbe = (rightProbe + 1) & arrayMask;
    // could be made more efficient with system array copying
    while (rightProbe != probe) {
      int leftOfRightProbe = (rightProbe - 1) & arrayMask;
      keys[rightProbe] = keys[leftOfRightProbe];
      values[rightProbe] = values[leftOfRightProbe];
      states[rightProbe] = (byte) (states[leftOfRightProbe] + 1);
      rightProbe = leftOfRightProbe;
    }
    keys[probe] = key;
    values[probe] = putAmount;
    states[probe] = state;
    size++;
  }

  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    // first probe is the last vacant cell before an occupied one
    int firstProbe = 0;
    while (states[firstProbe] > 0)
      firstProbe++;

    byte deletes = 0;
    int newProbe;
    // loop around the array from first to the end
    for (int probe = firstProbe; probe < length; probe++) {
      if (states[probe] > 0) {
        if (values[probe] <= thresholdValue) {
          states[probe] = 0;
          assert (deletes < MAX_STATE_ALLOWED);
          deletes++;
          size--;
        } else {
          if (states[probe] == 1 || deletes == 0) {
            // we need to keep this item in its place
            deletes = 0;
          } else {
            // we need to keep the item and mode it
            if (deletes >= states[probe] - 1)
              deletes = (byte) (states[probe] - 1);
            newProbe = (probe - deletes);
            keys[newProbe] = keys[probe];
            values[newProbe] = values[probe];
            states[newProbe] = (byte) (states[probe] - deletes);
            states[probe] = 0;
          }
        }
      }
    }
    // same loop from the 0 to the first with
    // modulo arithmetic for the probe calculation
    for (int probe = 0; probe < firstProbe; probe++) {
      if (states[probe] > 0) {
        if (values[probe] <= thresholdValue) {
          states[probe] = 0;
          assert (deletes < MAX_STATE_ALLOWED);
          deletes++;
          size--;
        } else {
          if (states[probe] == 1 || deletes == 0) {
            // we need to keep this item in its place
            deletes = 0;
          } else {
            // we need to keep the item and mode it
            if (deletes >= states[probe] - 1)
              deletes = (byte) (states[probe] - 1);
            newProbe = (probe - deletes) & arrayMask;
            keys[newProbe] = keys[probe];
            values[newProbe] = values[probe];
            states[newProbe] = (byte) (states[probe] - deletes);
            states[probe] = 0;
          }
        }
      }
    }
  }

}
