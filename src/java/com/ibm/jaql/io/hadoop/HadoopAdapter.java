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
package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConfigurable;

import com.ibm.jaql.io.Adapter;

/**
 * An adapter that is specific for Hadoop Input and Output formats.
 */
public interface HadoopAdapter
    extends Adapter, ConfSetter, JobConfigurable
{
  public final static String IMP                  = "com.ibm.jaql.io.hadoop.MapReduceExpr";

  public final static String storeRegistryVarName = IMP + ".sRegistry";

  static String              CONVERTER_NAME       = "converter";

  static String              CONFIGURATOR_NAME    = "configurator";
}
