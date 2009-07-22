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
package com.ibm.jaql.io.serialization.binary.temp;

import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;

/** Jaql's serializer for temporary files. Mainly used for serialization in between the map and
 * reduce phase. The file format is not stable. */
public final class TempBinaryFullSerializer extends DefaultBinaryFullSerializer // for the moment
{
  public TempBinaryFullSerializer(Schema schema) {
    super();
  }
}
