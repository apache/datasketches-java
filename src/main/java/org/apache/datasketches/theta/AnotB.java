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
 * Computes a set difference, A-AND-NOT-B, of two theta sketches.
 * This class includes both stateful and stateless operations.
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
   * This is part of a multistep, stateful AnotB operation and sets the given Theta sketch as the
   * first argument <i>A</i> of <i>A-AND-NOT-B</i>. This overwrites the internal state of this
   * AnotB operator with the contents of the given sketch.
   * This sets the stage for multiple following <i>notB</i> steps.
   *
   * <p>An input argument of null will throw an exception.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * Since it is very likely that a <i>null</i> is a programming error, we throw a an exception.</p>
   *
   * <p>An enpty input argument will set the internal state to empty.</p>
   *
   * <p>Rationale: An empty set is a mathematically legal concept. Although it makes any subsequent,
   * valid argument for B irrelvant, we must allow this and assume the user knows what they are
   * doing.</p>
   *
   * <p>Performing {@link #getResult(boolean)} just after this step will return a compact form of
   * the given argument.</p>
   *
   * @param skA The incoming sketch for the first argument, <i>A</i>.
   */
  public abstract void setA(Sketch skA);

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Theta sketch as the
   * second (or <i>n+1</i>th) argument <i>B</i> of <i>A-AND-NOT-B</i>.
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnotB operator.
   *
   * <p>An input argument of null or empty is ignored.</p>
   *
   * <p>Rationale: A <i>null</i> for the second or following arguments is more tollerable because
   * <i>A NOT null</i> is still <i>A</i> even if we don't know exactly what the null represents. It
   * clearly does not have any content that overlaps with <i>A</i>. Also, because this can be part of
   * a multistep operation with multiple <i>notB</i> steps. Other following steps can still produce
   * a valid result.</p>
   *
   * <p>Use {@link #getResult(boolean)} to obtain the result.</p>
   *
   * @param skB The incoming Theta sketch for the second (or following) argument <i>B</i>.
   */
  public abstract void notB(Sketch skB);

  /**
   * Gets the result of the mutistep, stateful operation AnotB that have been executed with calls
   * to {@link #setA(Sketch)} and ({@link #notB(Sketch)} or
   * {@link #notB(org.apache.datasketches.theta.Sketch)}).
   *
   * @param reset If <i>true</i>, clears this operator to the empty state after this result is
   * returned. Set this to <i>false</i> if you wish to obtain an intermediate result.
   * @return the result of this operation as an ordered, on-heap {@link CompactSketch}.
   */
  public abstract CompactSketch getResult(boolean reset);

  /**
   * Gets the result of this stateful set operation as a CompactSketch of the form based on
   * the input arguments.
   * The stateful input operations are {@link #setA(Sketch)} and {@link #notB(Sketch)}.
   *
   * @param dstOrdered If <i>true</i>, the result will be an ordered {@link CompactSketch}.
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   *
   * @param dstMem if not <i>null</i> the given Memory will be the target location of the result.
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @param reset If <i>true</i>, clears this operator to the empty state after this result is
   * returned. Set this to <i>false</i> if you wish to obtain an intermediate result.
   *
   * @return the result of this operation as a {@link CompactSketch} of the chosen form.
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
   * <p>If either argument is null an exception is thrown.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
   * In most cases <i>null</i> is a programming error due to a non-initialized object. </p>
   *
   * <p>With a null as the first argument we cannot know what the user's intent is and throw an
   * exception. With a null as the second argument for this method we must return a result and
   * there is no following possible viable arguments for the second argument so we thrown an
   * exception.</p>
   *
   * @param skA The incoming sketch for the first argument. It must not be null.
   * @param skB The incoming sketch for the second argument. It must not be null.
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
   * Thus, this is not an accumulating update and does not interact with the {@link #setA(Sketch)},
   * {@link #notB(Sketch)}, {@link #getResult(boolean)}, or
   * {@link #getResult(boolean, WritableMemory, boolean)} methods.</p>
   *
   * <p>If either argument is null an exception is thrown.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
   * In most cases <i>null</i> is a programming error due to a non-initialized object. </p>
   *
   * <p>With a null as the first argument we cannot know what the user's intent is and throw an
   * exception. With a null as the second argument for this method we must return a result and
   * there is no following possible viable arguments for the second argument so we thrown an
   * exception.</p>
   *
   * @param skA The incoming sketch for the first argument. It must not be null.
   * @param skB The incoming sketch for the second argument. It must not be null.
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
   * @see #aNotB(Sketch, Sketch)
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @deprecated Instead use {@link #aNotB(Sketch, Sketch)}.
   */
  @Deprecated
  public abstract void update(Sketch skA, Sketch skB);

  /**
   * Gets the result of the stateful operations, {@link #setA(Sketch)} and {@link #notB(Sketch)},
   * as an ordered CompactSketch on the Java heap.
   * This clears the state of this operator after the result is returned.
   * @return the result of the stateful operations as an ordered CompactSketch on the Java heap.
   * @deprecated Instead use {@link #getResult(boolean)} or
   * {@link #getResult(boolean, WritableMemory, boolean)}.
   */
  @Deprecated
  public CompactSketch getResult() {
    return getResult(true, null, true);
  }

  /**
   * Gets the result of the stateful operations {@link #setA(Sketch)} and {@link #notB(Sketch)}.
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
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    return getResult(dstOrdered, dstMem, true);
  }

}
