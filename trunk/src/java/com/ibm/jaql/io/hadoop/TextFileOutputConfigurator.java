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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class TextFileOutputConfigurator extends FileOutputConfigurator {

	@Override
	protected void registerSerializers(JobConf conf) {
		conf.setMapOutputKeyClass(JsonHolder.class);
		conf.setMapOutputValueClass(JsonHolder.class);
		conf.setOutputKeyClass(NullWritable.class);
	    conf.setOutputValueClass(Text.class);
	}
}