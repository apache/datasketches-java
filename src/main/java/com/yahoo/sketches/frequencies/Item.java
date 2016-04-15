/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

/**
 * Defines an Item usable by the FrequentItems sketch.
 */
public interface Item {
  
  /**
   * Gets an identifier for this Item that is uniquely associated with the content of this item.
   * Thus, if (item1.equals(item2) == true), the two identifiers must be equal.
   * The case where (item1.getID() == item2.getID()) and (item1.equals(item2) == false) constitutes
   * a collision of identifiers and will degrade the accuracy of the frequency estimation and 
   * should be avoided or at least be made very rare.  
   * 
   * <p>To improve performance, the owner of the Item object should compute or
   * assign this identifier when the Item object is created and cash this identifier with the Item
   * object.</p>
   *  
   * @return a unique item identifier
   */
  long getID();
  
  /**
   * The owner of the Item can use whatever serialization approach they desire, but must be aware
   * that serialization performance and size will have dramatic influence on overall system
   * performance, especially with large data. 
   * 
   * <p>The deserialization method is normally a static method and also must be provided by the
   * owner of this Item. 
   * 
   * @return a byte array that can later be deserialized into the original object represented by
   * this Item.
   */
  byte[] serializeToByteArray();
  
}
