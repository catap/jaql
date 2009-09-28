/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.io.serialization;

import java.io.IOException;

import com.ibm.jaql.json.type.JsonValue;

/**
 * Superclass for serializers of <code>JsonValue</code>s of known type and
 * encoding. Each <code>BasicSerializer</code> is associated with a particular
 * implementing class <code>T</code> of {@link JsonValue}. It can only read and
 * write values of type <code>T</code>. Moreover, values that are read have to
 * be written by the same <code>BasicSerializer</code>.
 * 
 * See {@link FullSerializer} serializers that extract type information from the
 * input.
 * 
 * @param <In> type of input
 * @param <Out> type of output
 * @param <T> type of value to work on
 */
public abstract class BasicSerializer<In, Out, T extends JsonValue> implements
    Serializer<In, Out, T> {

  @Override
  public void skip(In in) throws IOException {
    read(in, null);
  }

  @Override
  public int compare(In in1, In in2) throws IOException {
    T v1 = read(in1, null);
    T v2 = read(in2, null);
    return v1.compareTo(v2);
  }

  @Override
  public void copy(In in, Out out) throws IOException {
    T v = read(in, null);
    write(out, v);
  }
}
