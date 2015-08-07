package com.yahoo.sketches.hll;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import java.util.Arrays;

/**
 */
public class OnHeapHashFields implements Fields
{
  private final Preamble preamble;
  private final int switchToDenseSize;

  private int[] fields;
  private int mask;
  private int numElements;
  private int growthBound;

  public OnHeapHashFields(Preamble preamble) {
    this.preamble = preamble;
    this.switchToDenseSize = HashUtils.MAX_HASH_SIZE[preamble.getLogConfigK()];
    resetFields(16);
  }

  private void resetFields(int size) {
    this.fields = new int[size];
    Arrays.fill(this.fields, -1);
    this.mask = fields.length - 1;
    this.numElements = 0;
    this.growthBound = 3 * (fields.length >>> 2);
  }

  @Override
  public Preamble getPreamble()
  {
    return preamble;
  }

  @Override
  public Fields updateBucket(int key, byte val, UpdateCallback callback) {
    return updateBucket(key, val, HashUtils.pairOfKeyAndVal(key, val), callback);
  }

  private Fields updateBucket(int key, byte val, int newField, UpdateCallback callback) {
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

    if (numElements >= growthBound) {
      UpdateCallback noopCB = new NoopUpdateCallback();
      int[] oldFields = fields;
      if (oldFields.length == switchToDenseSize) {
        Fields retVal = new OnHeapFields(preamble);
        BucketIterator iter = getBucketIterator();
        while (iter.next()) {
          retVal.updateBucket(iter.getKey(), iter.getValue(), noopCB);
        }
        return retVal;
      } else {
        resetFields(oldFields.length << 1);
        for (int oldField : oldFields) {
          updateBucket(HashUtils.keyOfPair(oldField), HashUtils.valOfPair(oldField), oldField, noopCB);
        }
      }
    }

    return this;
  }

  @Override
  public int intoByteArray(byte[] array, int offset)
  {
    int numBytesNeeded = numBytesToSerialize();
    if (array.length - offset < numBytesNeeded) {
      throw new IllegalArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    Memory mem = new NativeMemory(array);
    mem.putByte(offset++, Fields.HASH_SPARSE_VERSION);

    for (int field : fields) {
      mem.putInt(offset, field);
      offset += 4;
    }

    return offset;
  }

  @Override
  public int numBytesToSerialize()
  {
    return 1 + (fields.length << 2);
  }

  @Override
  public Fields toCompact()
  {
    return OnHeapImmutableCompactFields.fromFields(this);
  }

  @Override
  public BucketIterator getBucketIterator()
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
}
