package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.ReadOnlyMemoryException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapUpdateSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test(expectedExceptions = ReadOnlyMemoryException.class)
  public void wrapAndTryUpdatingUpdateSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.toByteArray()).asReadOnlyBuffer());
    UpdateSketch sketch = (UpdateSketch) Sketch.wrap(mem);
    sketch.update(2);
  }

  @Test
  public void wrapCompactSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyUpdateSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyCompactSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

}
