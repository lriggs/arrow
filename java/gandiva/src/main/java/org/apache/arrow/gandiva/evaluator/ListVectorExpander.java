/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.vector.complex.ListVector;

/**
 * This class provides the functionality to expand output vectors using a callback mechanism from
 * gandiva.
 */
public class ListVectorExpander {
  private final ListVector[] vectors;

  public ListVectorExpander(ListVector[] vectors) {
    this.vectors = vectors;
  }

  /**
   * Result of vector expansion.
   */
  public static class ExpandResult {
    public long address;
    public long capacity;
    public long offsetaddress;
    public long offsetcapacity;
    public long validityaddress;
    public long outervalidityaddress;

    /**
     * fdsfsdfds.
     * @param address dsfds
     * @param capacity dfsdf
     * @param offsetad dsfdsfsd
     * @param offsetcap dfsfs
     * 
     */
    public ExpandResult(long address, long capacity, long offsetad, long offsetcap, long outValidAdd, long validAdd) {
      this.address = address;
      this.capacity = capacity;
      this.offsetaddress = offsetad;
      this.offsetcapacity = offsetcap;
      this.outervalidityaddress = outValidAdd;
      this.validityaddress = validAdd;
    }
  }

  /**
   * Expand vector at specified index. This is used as a back call from jni, and is only
   * relevant for ListVectors.
   *
   * @param index index of buffer in the list passed to jni.
   * @param toCapacity the size to which the buffer should be expanded to.
   *
   * @return address and size  of the buffer after expansion.
   */
  public ExpandResult expandOutputVectorAtIndex(int index, long toCapacity) {
    if (index >= vectors.length || vectors[index] == null) {
      throw new IllegalArgumentException("invalid index " + index);
    }


    //ArrowBuf ab = vectors[index].getValidityBuffer();
    //String s = "Before validity = [";
    //for (int i = 0; i < 20; i++) {
    //  s += ab.getInt(i) + ",";
    //}
    //System.out.println(s);


    int valueBufferIndex = 1;
    int validBufferIndex = 0;
    ListVector vector = vectors[index];
    while (vector.getDataVector().getFieldBuffers().get(valueBufferIndex).capacity() < toCapacity) {
      //vector.reAlloc();
      vector.getDataVector().reAlloc();
    }
    System.out.println("LR Expanding ListVector. New capacity=" +
        vector.getDataVector().getFieldBuffers().get(valueBufferIndex).capacity());
    System.out.println("LR Expanding ListVector. new data is ");
    
    /*ArrowBuf ab2 = vector.getValidityBuffer();
    s = "After validity = [";
    for (int i = 0; i < 20; i++) {
      s += ab2.getInt(i) + ",";
    }
    System.out.println(s);*/
    /*ArrowBuf ab = vector.getOffsetBuffer();
    String s = "offsetBuffer = [";
    for (int i = 0; i < 20; i++) {
      s += ab.getInt(i) + ",";
    }
    System.out.println(s);
    */
    return new ExpandResult(
        vector.getDataVector().getFieldBuffers().get(valueBufferIndex).memoryAddress(),
        vector.getDataVector().getFieldBuffers().get(valueBufferIndex).capacity(),
        vector.getOffsetBuffer().memoryAddress(),
        vector.getOffsetBuffer().capacity(),
        vector.getValidityBuffer().memoryAddress(),
        vector.getDataVector().getFieldBuffers().get(validBufferIndex).memoryAddress());
  }

}
