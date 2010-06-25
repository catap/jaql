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
package com.ibm.jaql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * A JUnit test suite to run all of the jaql script JUnit tests. 

 * It would be nice if we could eliminate this class and all the little Test* classes.
 * We could easily generate these classes from the script names.  Maybe TestNG has a better way.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestCore.class,
  TestExamples.class,
  TestHashtable.class,
  TestInputSplits.class,
  TestLongList.class,
  TestModule.class,
  TestOptions.class,
  TestRegistry.class,
  TestRng.class,
  TestSchema.class,
  TestSchemaPrinting.class,
  TestStorage.class,
  TestStorageTemp.class,
  TestStorageText.class
})
public class JaqlScriptTestSuite
{
}
