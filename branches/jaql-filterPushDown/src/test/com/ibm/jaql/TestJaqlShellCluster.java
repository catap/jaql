package com.ibm.jaql;

import org.junit.Test;
/**
 * Unit test for Jaql shell with existing cluster.
 */
public class TestJaqlShellCluster extends JaqlShellBaseTestCase {
  /**
   * Tests interactive mode.
   */
  @Test
  public void interactiveCluster() {
    verify(JaqlBaseTestCase.getPathname("jaqlShellInteractiveQueries"),
           "jaqlShellInteractiveCluster",
           null,
           new String[]{"-c"});
  }

  /**
   * Test options.
   */
  @Test
  public void options() {
    verify("jaqlShellOptions", new String[]{"-c", "-b", "-o", "del"});
  }
}
