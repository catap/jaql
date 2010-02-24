package com.ibm.jaql;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
/**
 * Unit test for Jaql shell with mini cluster.
 */
public class TestJaqlShell extends JaqlShellBaseTestCase {

  /**
   * Tests interactive mode.
   */
  @Test
  public void interactive() {
    List<RegexReplacement> rrs = new ArrayList<RegexReplacement>();
    /*
     * Replace backslash \ with slash /
     */
    rrs.add(new RegexReplacement("\\\\tmp\\\\jaql\\\\dfs\\\\dfs\\\\data\\\\data(\\d)",
                                "/tmp/jaql/dfs/dfs/data/data$1"));
    /*
     * How many times the line is printed depends on environment factors such as
     * machine performance. So it is removed.
     */
    rrs.add(new RegexReplacement("Waiting for the Mini HDFS Cluster to start\\.\\.\\.",
                                ""));
    verify("jaqlShellInteractive", rrs, null);
  }
  
  /**
   * Tests batch mode.
   */
  @Test
  public void batch() {
    verify("jaqlShellBatch", new String[]{"-b"});
  }
  
}
