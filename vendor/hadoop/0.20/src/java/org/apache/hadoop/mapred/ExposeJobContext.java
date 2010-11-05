/*
 * Copyright (C) IBM Corp. 2010.
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
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Progressable;

/**
 * We need access to JobContext, but it's constructor is package protected.
 * The new mapreduce api does not have this problem.
 * When we move to the new api, this code should be removed.
 */
public class ExposeJobContext extends JobContext
{
  public ExposeJobContext(JobConf conf, JobID jobId, Progressable progress)
  {
    super(conf, jobId, progress);
  }

  public ExposeJobContext(JobConf conf, JobID jobId)
  {
    super(conf, jobId);
  }
}
