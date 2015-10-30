package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 */
public class FieldsTest
{
  private static final Fields.UpdateCallback cb = new Fields.UpdateCallback()
  {
    @Override
    public void bucketUpdated(int bucket, byte oldVal, byte newVal)
    {

    }
  };

  @DataProvider(name = "updatableFields")
  public Object[][] getFields() {
    Preamble preamble = Preamble.fromLogK(10);

    return new Object[][] {
        { new OnHeapFields(preamble) },
        { new OnHeapHashFields(preamble, 16, 0x2<<preamble.getLogConfigK(), new DenseFieldsFactory()) },
        { new OnHeapCompressedFields(preamble) }
    };
  }

  @Test(dataProvider = "updatableFields")
  public void testUpdateBucketBreadthFirst(Fields fields) {
    Random rand = new Random(1234L);
    int numBuckets = fields.getPreamble().getConfigK();

    List<Integer> indexes = new ArrayList<>(numBuckets);
    int[] vals = new int[numBuckets];
    for (int i = 0; i < numBuckets; ++i) {
      indexes.add(i);
    }
    for (int val = 0; val < 64; ++val) {
      Collections.shuffle(indexes, rand);

      for (int i = 0; i < numBuckets; ++i) {
        int index = indexes.get(i);
        fields = fields.updateBucket(index, (byte) val, cb);
        vals[index] = Math.max(vals[index], val);

        BucketIterator iter = fields.getBucketIterator();
        while (iter.next()) {
          int bucketId = iter.getKey();
          if (iter.getValue() != vals[bucketId]) {
            Assert.fail(String.format("bucket[%s]: %d != %d", bucketId, iter.getValue(), vals[bucketId]));
          }
        }
      }
    }
  }

  @Test(dataProvider = "updatableFields")
  public void testUpdateBucketDepthFirst(Fields fields) {
    Random rand = new Random(1234L);
    int numBuckets = fields.getPreamble().getConfigK();

    List<Byte> valsToInsert = new ArrayList<>(64);
    int[] actualVals = new int[numBuckets];
    for (byte i = 0; i < 64; ++i) {
      valsToInsert.add(i);
    }
    for (int bucket = 0; bucket < numBuckets; ++bucket) {
      Collections.shuffle(valsToInsert, rand);

      for (Byte val : valsToInsert) {
        fields = fields.updateBucket(bucket, val, cb);
        actualVals[bucket] = Math.max(actualVals[bucket], val);

        BucketIterator iter = fields.getBucketIterator();
        while (iter.next()) {
          int bucketId = iter.getKey();
          if (iter.getValue() != actualVals[bucketId]) {
            Assert.fail(String.format("bucket[%s]: %d != %d", bucketId, iter.getValue(), actualVals[bucketId]));
          }
        }
      }
    }
  }
}
