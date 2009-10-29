/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.util.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ibm.jaql.io.OutputAdapter;

public class TestJaqlShellArguments {

  @Test
  public void useExistingCluster() {
    assertTrue(JaqlShellArguments.parseArgs("-c").useExistingCluster);
    assertFalse(JaqlShellArguments.parseArgs().useExistingCluster);
  }

  /*
   * It has some dependencies on some files in jaql project root directory.
   */
  @Test
  public void jars() throws Exception {
    verifyMultipleValueOption("-j", ",", "jars", ".classpath", ".project");
  }

  @Test
  public void outoptions() {
    JaqlShellArguments arguments = JaqlShellArguments.parseArgs("-b",
                                                                "-o",
                                                                "xml");
    assertTrue(arguments.outputAdapter instanceof OutputAdapter);
  }

  /*
   * It has some dependencies on some directories in jaql project root
   * directory.
   */
  @Test
  public void searchPath() throws Exception {
    verifyMultipleValueOption("-jp", ":", "searchPath", "conf", "lib");
  }

  @Test
  public void hdfsDir() {
    assertEquals(JaqlShellArguments.DEFAULT_HDFS_DIR,
                 JaqlShellArguments.parseArgs().hdfsDir);
    String hdfsDir = "/tmp/dfs";
    assertEquals("/tmp/dfs",
                 JaqlShellArguments.parseArgs("-d", hdfsDir).hdfsDir);
  }

  @Test
  public void noNodes() {
    assertEquals(JaqlShellArguments.DEFAULT_NUM_NODES,
                 JaqlShellArguments.parseArgs().numNodes);
    assertEquals(2, JaqlShellArguments.parseArgs("-n", "2").numNodes);
  }

  @Test
  public void batchMode() {
    assertEquals(JaqlShellArguments.DEFAULT_BATCH_MODE,
                 JaqlShellArguments.parseArgs().batchMode);
    assertEquals(true, JaqlShellArguments.parseArgs("-b").batchMode);
  }

  private void verifyMultipleValueOption(String option,
                                         String sep,
                                         String fieldName,
                                         String... values) throws Exception {
    List<String> expected = new ArrayList<String>();
    StringBuilder vs = new StringBuilder();
    String delimiter = "";
    for (String v : values) {
      expected.add(v);
      vs.append(delimiter);
      vs.append(v);
      delimiter = sep;
    }

    JaqlShellArguments arguments = JaqlShellArguments.parseArgs(option,
                                                                vs.toString());
    List<String> actual = new ArrayList<String>();
    for (String jar : (String[]) JaqlShellArguments.class.getDeclaredField(fieldName)
                                                         .get(arguments)) {
      actual.add(jar);
    }
    assertEquals(expected, actual);
  }

}
