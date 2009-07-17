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
package com.foobar.store;

import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.json.type.JsonValue;


/**
 * 
 */
public class ToJSONTxtConverter extends ToJSONSeqConverter
{

  /*
   * (non-Javadoc)
   * 
   * @see com.foobar.store.ToJSONSeqConverter#convertItemToString(com.ibm.jaql.json.type.Item)
   */
  @Override
  protected String convertItemToString(JsonValue val) throws Exception
  {
    String s = super.convertItemToString(val);
    s = s.replace("\r", ""); // this loses information in case there are newlines in the data
    s = s.replace("\n", "");
    return s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.foobar.store.ToJSONSeqConverter#createKeyTarget()
   */
  @Override
  public WritableComparable<?> createKeyTarget()
  {
    return null;
  }
}
