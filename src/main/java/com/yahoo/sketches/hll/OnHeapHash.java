package com.yahoo.sketches.hll;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import java.util.Arrays;

/**
 */
public class OnHeapHash
{
  private int[] fields;
  private int mask;
  private int numElements;

  OnHeapHash(int startSize) {
    resetFields(startSize);
  }

  void resetFields(int size) {
    this.fields = new int[size];
    Arrays.fill(this.fields, -1);
    this.mask = fields.length - 1;
    this.numElements = 0;
  }

  int[] getFields()
  {
    return fields;
  }

  public int getNumElements()
  {
    return numElements;
  }

  void updateBucket(int key, byte val, Fields.UpdateCallback callback) {
    updateBucket(key, val, HashUtils.pairOfKeyAndVal(key, val), callback);
  }

  private int updateBucket(int key, byte val, int newField, Fields.UpdateCallback callback) {
    int probe = key & mask;
    int field = fields[probe];
    while (field != HashUtils.NOT_A_PAIR && key != HashUtils.keyOfPair(field)) {
      probe = (probe + 1) & mask;
      field = fields[probe];
    }

    if (field == HashUtils.NOT_A_PAIR) {
      fields[probe] = newField;
      callback.bucketUpdated(key, (byte) 0, val);
      ++numElements;
    }

    byte oldVal = HashUtils.valOfPair(field);
    if (oldVal < val) {
      fields[probe] = newField;
      callback.bucketUpdated(key, oldVal, val);
      ++numElements;
    }

    return numElements;
  }

  int intoByteArray(byte[] array, int offset)
  {
    int numBytesNeeded = numBytesToSerialize();
    if (array.length - offset < numBytesNeeded) {
      throw new IllegalArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    Memory mem = new NativeMemory(array);

    for (int field : fields) {
      mem.putInt(offset, field);
      offset += 4;
    }

    return offset;
  }

  int numBytesToSerialize()
  {
    return (fields.length << 2);
  }

  BucketIterator getBucketIterator()
  {
    return new BucketIterator()
    {
      private int i = -1;

      @Override
      public boolean next()
      {
        ++i;
        while (i < fields.length && fields[i] == HashUtils.NOT_A_PAIR) {
          ++i;
        }
        return i < fields.length;
      }

      @Override
      public int getKey()
      {
        return HashUtils.keyOfPair(fields[i]);
      }

      @Override
      public byte getValue()
      {
        return HashUtils.valOfPair(fields[i]);
      }
    };
  }

  void boostrap(int[] fields) {
    for (int field : fields) {
      if (field != HashUtils.NOT_A_PAIR) {
        updateBucket(HashUtils.keyOfPair(field), HashUtils.valOfPair(field), field, Fields.NOOP_CB);
      }
    }
  }
}
