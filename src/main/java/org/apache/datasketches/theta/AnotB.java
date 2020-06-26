/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The API for the set difference operation <i>A and not B</i> operations.
 * This class include both stateful and stateless operations.
 *
 * <p>The stateful operation is as follows:</p>
 * <pre><code>
 * AnotB anotb = SetOperationBuilder.buildAnotB();
 *
 * anotb.setA(Sketch skA); //The first argument.
 * anotb.notB(Sketch skB); //The second (subtraction) argument.
 * anotb.notB(Sketch skC); // ...any number of additional subtractions...
 * anotb.getResult(false); //Get an interim result.
 * anotb.notB(Sketch skD); //Additional subtractions.
 * anotb.getResult(true);  //Final result and resets the AnotB operator.
 * </code></pre>
 *
 * <p>The stateless operation is as follows:</p>
 * <pre><code>
 * AnotB anotb = SetOperationBuilder.buildAnotB();
 *
 * CompactSketch csk = anotb.aNotB(Sketch skA, Sketch skB);
 * </code></pre>
 *
 * <p>Calling the <i>setA</i> operation a second time essentially clears the internal state and loads
 * the new sketch.</p>
 *
 * <p>The stateless and stateful operations are independent of each other with the exception of
 * sharing the same update hash seed loaded as the default seed or specified by the user as an
 * argument to the builder.</p>
 *
 * @author Lee Rhodes
 */
public abstract class AnotB extends SetOperation {

  @Override
  public Family getFamily() {
    return Family.A_NOT_B;
  }

  /**
   * This is a stateful input operation. This method sets the given Sketch as the first
   * argument <i>A</i> of a stateful <i>AnotB</i> operation. This overwrites the internal state of
   * this AnotB operator with the contents of the given sketch. This sets the stage for multiple
   * stateful subsequent {@link #notB(Sketch)} operations. The ultimate result is obtained using
   * the {@link #getResult(boolean)} or {@link #getResult(boolean, WritableMemory, boolean)}.
   *
   * <p>An input argument of null will throw an exception.</p>
   *
   * @param skA The incoming sketch for the first argument, <i>A</i>.
   */
  public abstract void setA(Sketch skA);

  /**
   * Performs a stateful <i>AND NOT</i> operation with the existing internal state of this AnotB
   * operator. Use {@link #getResult(boolean)} or {@link #getResult(boolean, WritableMemory, boolean)}
   * to obtain the result.
   *
   * <p>An input argument of null or empty is ignored.</p>
   *
   * @param skB The incoming sketch for the second (or following) argument <i>B</i>.
   */
  public abstract void notB(Sketch skB);

  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap.
   * @param reset If true, clears this operator to the empty state after result is returned.
   * @return the result of this operation as a CompactSketch.
   */
  public abstract CompactSketch getResult(boolean reset);

  /**
   * Gets the result of this stateful set operation as a CompactSketch of the chosen form. The
   * stateful input operations are {@link #setA(Sketch)} and {@link #notB(Sketch)}.
   *
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   *
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @param reset If true, clears this operator to the empty state after result is returned.
   *
   * @return the result of this set operation as a CompactSketch of the chosen form
   */
  public abstract CompactSketch getResult(boolean dstOrdered, WritableMemory dstMem, boolean reset);

  /**
   * Perform A-and-not-B set operation on the two given sketches and return the result as an
   * ordered CompactSketch on the heap.
   *
   * <p>This a stateless operation and has no impact on the internal state of this operator.
   * Thus, this is not an accumulating update and does not interact with the {@link #setA(Sketch)},
   * {@link #notB(Sketch)}, {@link #getResult(boolean)}, or
   * {@link #getResult(boolean, WritableMemory, boolean)} methods.</p>
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @return an ordered CompactSketch on the heap
   */
  public CompactSketch aNotB(final Sketch skA, final Sketch skB) {
    return aNotB(skA, skB, true, null);
  }

  /**
   * Perform A-and-not-B set operation on the two given sketches and return the result as a
   * CompactSketch.
   *
   * <p>This a stateless operation and has no impact on the internal state of this operator.
   * Thus, this is not an accumulating update and does not interact with the {@link #setA(Sketch)}
   * or {@link #notB(Sketch)} methods.</p>
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return the result as a CompactSketch.
   */
  public abstract CompactSketch aNotB(Sketch skA, Sketch skB, boolean dstOrdered,
      WritableMemory dstMem);

  //Deprecated methods

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @deprecated Instead use {@link #aNotB(Sketch, Sketch)}.
   */
  @Deprecated
  public abstract void update(Sketch skA, Sketch skB);

  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap.
   * This clears the state of this operator after the result is returned.
   * @return the result of this operation as an ordered CompactSketch on the Java heap.
   * @deprecated Instead use {@link #getResult(boolean)} or
   * {@link #getResult(boolean, WritableMemory, boolean)}.
   */
  @Deprecated
  public abstract CompactSketch getResult();

  /**
   * Gets the result of this set operation as a CompactSketch of the chosen form.
   * This clears the state of this operator after the result is returned.
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   *
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @return the result of this set operation as a CompactSketch of the chosen form.
   * @deprecated Instead use {@link #getResult(boolean)} or
   * {@link #getResult(boolean, WritableMemory, boolean)}.
   */
  @Deprecated
  public abstract CompactSketch getResult(boolean dstOrdered, WritableMemory dstMem);

}
