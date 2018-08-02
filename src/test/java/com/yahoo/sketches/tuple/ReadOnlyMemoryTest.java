package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesReadOnlyException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingSketch() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    ArrayOfDoublesUpdatableSketch sketch2 = (ArrayOfDoublesUpdatableSketch)
        ArrayOfDoublesSketches.wrapSketch(Memory.wrap(sketch1.toByteArray()));
    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    sketch2.toByteArray();
    boolean thrown = false;
    try {
      sketch2.update(2, new double[] {1});
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    try {
      sketch2.trim();
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void heapifyAndUpdateSketch() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    // downcasting is not recommended, for testing only
    ArrayOfDoublesUpdatableSketch sketch2 = (ArrayOfDoublesUpdatableSketch)
        ArrayOfDoublesSketches.heapifySketch(Memory.wrap(sketch1.toByteArray()));
    sketch2.update(2, new double[] {1});
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void wrapAndTryUpdatingUnionEstimationMode() {
    int numUniques = 10000;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch1.update(key++, new double[] {1});
    }
    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.update(sketch1);
    ArrayOfDoublesUnion union2 = ArrayOfDoublesSketches.wrapUnion(Memory.wrap(union1.toByteArray()));
    ArrayOfDoublesSketch resultSketch = union2.getResult();
    Assert.assertTrue(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), numUniques, numUniques * 0.04);

    // make sure union update actually needs to modify the union
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch2.update(key++, new double[] {1});
    }

    boolean thrown = false;
    try {
      union2.update(sketch2);
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void heapifyAndUpdateUnion() {
    int numUniques = 10000;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch1.update(key++, new double[] {1});
    }
    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.update(sketch1);
    ArrayOfDoublesUnion union2 = ArrayOfDoublesSketches.heapifyUnion(Memory.wrap(union1.toByteArray()));
    ArrayOfDoublesSketch resultSketch = union2.getResult();
    Assert.assertTrue(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), numUniques, numUniques * 0.04);

    // make sure union update actually needs to modify the union
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch2.update(key++, new double[] {1});
    }
    union2.update(sketch2);
  }

  @Test
  public void wrapAndTryUpdatingUnionV0_9_1() throws Exception {
    byte[] bytes = TestUtil.readBytesFromFile(getClass().getClassLoader()
        .getResource("ArrayOfDoublesUnion_v0.9.1.bin").getFile());
    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.wrap(Memory.wrap(bytes));
    ArrayOfDoublesCompactSketch result = union2.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);

    boolean thrown = false;
    try {
      union2.reset();
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

}
