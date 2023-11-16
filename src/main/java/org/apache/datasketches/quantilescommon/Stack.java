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

package org.apache.datasketches.quantilescommon;

import java.util.ArrayList;

import org.apache.datasketches.common.SketchesStateException;

/**
 * A classic LIFO stack based on ArrayList (as opposed to Vector).
 * All of the methods of ArrayList are available.
 */
public class Stack<E> extends ArrayList<E> {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an empty stack.
   */
  public Stack() { }

  /**
   * Pushes an item onto the stack
   * @param item the given item
   * @return the given element
   */
  public E push(final E item) {
    add(item);
    return item;
  }

  /**
   * Removes the item at the top of the stack.
   * @return the item at the top of the stack.
   */
  public E pop() {
    final E item = peek();
    remove(size() - 1);
    return item;
  }

  /**
   * Allows examination of the top item without removing it.
   * @return the top item without removing it
   */
  public E peek() {
    final int len = size();
    if (len == 0) { throw new SketchesStateException("Stack is empty"); }
    return get(len - 1);
  }

}
