package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 */
public class FieldsTest
{
  @DataProvider(name = "updatableFields")
  public Object[][] getFields() {
    Preamble preamble = Preamble.createSharedPreamble(10);

    return new Object[][] {
        { new OnHeapFields(preamble) },
        { new OnHeapHashFields(preamble) }
    };
  }

  @Test(dataProvider = "updatableFields")
  public void testUpdateBucket(Fields fields) {
    int numBuckets = fields.getPreamble().getConfigK();

    List<Integer> indexes = new ArrayList<>(numBuckets);
    int[] vals = new int[numBuckets];
    for (int i = 0; i < numBuckets; ++i) {
      indexes.add(i);
    }
    for (int val = 0; val < 64; ++val) {
      Collections.shuffle(indexes);

      for (int i = 0; i < numBuckets; ++i) {
        int index = indexes.get(i);
        fields = fields.updateBucket(index, (byte) val);
        vals[index] = Math.max(vals[index], val);

        BucketIterator iter = fields.getBucketIterator();
        while (iter.next()) {
          if (iter.getValue() != vals[iter.getKey()]) {
            Assert.fail(String.format("%d x %d", val, i));
          }
        }
      }
    }
  }
}
