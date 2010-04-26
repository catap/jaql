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
package com.ibm.jaql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Skeleton unit test for Jaql shell by launching <i>bin/jaqlshell</i>.
 */
public abstract class JaqlShellBaseTestCase {

  private static boolean isWindows;

  static {
    isWindows = System.getProperty("os.name").toLowerCase().contains("win");
  }

  /*
   * Assumes that the user's current working directory is the root directory of
   * jaql project.
   */
  private static String JAQL_HOME = System.getProperty("user.dir");
  private static String JAQL_SHELL_LAUNCHER = JaqlBaseTestCase.TEST_CACHE_DATA
      + File.separator + "jaqlShellLauncher.sh";

  /**
   * Verifies that Jaql Shell produce the same output as expected.
   * 
   * @param prefix prefix
   * @param arguments Jaql Shell arguments except input file
   * @see #verify(String, String, List, String[])
   */
  void verify(String prefix, String[] arguments) {
    verify(prefix, null, arguments);
  }

  /**
   * Verifies that Jaql Shell produces the same output as expected.
   * 
   * @param prefix prefix
   * @param regexReplacements regular expression replacements
   * @param arguments Jaql Shell arguments except input file
   * @see #verify(String, String, Map, String[])
   */
  void verify(String prefix,
              List<RegexReplacement> regexReplacements,
              String[] arguments) {
    verify(JaqlBaseTestCase.getQueryPathname(prefix),
           prefix,
           regexReplacements,
           arguments);
  }

  /**
   * Verifies that Jaql Shell produce the same output as expected.
   * 
   * @param queryFileName query file name without file extension
   * @param prefix prefix
   * @param regexReplacements regular expression replacements
   * @param arguments Jaql Shell arguments except input file
   * @see #compareResults(String, String, List)
   */
  void verify(String queryFileName,
                      String prefix,
                      List<RegexReplacement> regexReplacements,
                      String[] arguments) {
    try {
      String tmpFileName = JaqlBaseTestCase.getTmpPathname(prefix);
      String goldFileName = JaqlBaseTestCase.getGoldPathname(prefix);

      if (arguments == null)
        arguments = new String[0];
      int len = arguments.length;
      String[] withInputFile = new String[len + 1];
      withInputFile[len] = queryFileName;
      System.arraycopy(arguments, 0, withInputFile, 0, len);

      runJaqlShell(withInputFile, tmpFileName);
      boolean result = compareResults(tmpFileName,
                                      goldFileName,
                                      regexReplacements);
      assertTrue("\n\nFound difference between gold file and tmp file", result);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      fail(e.getMessage());
      System.err.println("\n\nRun jaqlshell with failure");
    }
  }

  /**
   * Compares the tmp file and gold file. The content of the tmp file is
   * normalized before doing comparison.
   * 
   * @param tmpFileName tmp file name
   * @param goldFileName gold file name
   * @param regexReplacements regular expression replacements
   * @return <tt>true</tt> if the tmp file and gold file are the same;
   *         <tt>false</tt> otherwise.
   * @throws IOException
   * @see #normalize(String, List)
   */
  private static boolean compareResults(String tmpFileName,
                                        String goldFileName,
                                        List<RegexReplacement> regexReplacements) throws IOException {
    normalize(tmpFileName, regexReplacements);
    return JaqlBaseTestCase.compareResults(tmpFileName, goldFileName);
  }

  /**
   * Normalizes the text content of the given file. First, it does the text
   * replacements line by line for the original file to produce a new file.
   * <ol>
   * <li>Read a line <i>L</i> from the original file.</li>
   * <li>Try to get a replacement <i>R</i> from the <tt>regexReplacements</tt>
   * (Regular expression replacement with small index is tried first). If all
   * the replacements has been accessed, go to Step 6.</li>
   * <li>If the regular expression in <i>R</i> matches <i>L</i> or part of
   * <i>L</i>, go to Step 4. Otherwise, goto Step 2.</li>
   * <li>Replace all the parts of <i>L</i> with <i>R</i>'s replacement.</li>
   * <li>
   * If the replaced result for one line is empty string, then the whole line is
   * not written to the new file. Otherwise, the replaced result is written to
   * the new file. Go to step 1.</li>
   * <li>Write <i>L</i> to the new file.</li>
   * </ol>
   * <p>
   * Second,the original file is renamed to <tt>fileName.orig</tt>. The new file
   * is renamed to the original name of the given file.
   * 
   * 
   * @param fileName file name
   * @param regexReplacements regular expression replacements. Regular
   *          expression replacement with small index is tried first.
   * @throws IOException
   */
  private static void normalize(String fileName,
                                 List<RegexReplacement> regexReplacements) throws IOException {
    if (regexReplacements != null) {
      String normFileName = fileName + ".norm";
      BufferedReader reader = null;
      BufferedWriter writer = null;
      try {
        reader = new BufferedReader(new FileReader(fileName));
        writer = new BufferedWriter(new FileWriter(normFileName));
        String line;
        String newLine = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
          boolean replaced = false;
          for (RegexReplacement replacement : regexReplacements) {
            Pattern pattern = Pattern.compile(replacement.getRegex());
            Matcher matchter = pattern.matcher(line);
            if (matchter.find()) {
              String replacedLine = matchter.replaceAll(replacement.getReplacement());
              if (!replacedLine.equals("")) {
                writer.write(replacedLine + newLine);
              }
              replaced = true;
              break;
            } 
          }
          if (!replaced)
            writer.write(line + newLine);
        }
        writer.flush();
      } finally {
        if (reader != null ) 
          try { reader.close(); } catch (IOException e) { e.printStackTrace(); }
        if (writer != null ) 
          try { writer.close(); } catch (IOException e) { e.printStackTrace(); }
      }
      String origFileName = fileName + ".orig";
      assertTrue(fileName + " can't be renamed to " + origFileName,
                 new File(fileName).renameTo(new File(origFileName)));
      assertTrue(normFileName + " can't be renamed to " + fileName,
                 new File(normFileName).renameTo(new File(fileName)));
    }
  }

  /**
   * Runs Jaql shell with the given arguments.
   * 
   * @param arguments script arguments
   * @param tmp tmp file name
   * @throws Exception
   */
  private void runJaqlShell(String[] arguments, String tmp) throws Exception {
    int len = arguments.length;
    String[] complete = new String[len + 2];
    complete[0] = JAQL_SHELL_LAUNCHER;
    complete[1] = JAQL_HOME;
    System.arraycopy(arguments, 0, complete, 2, len);
    BashScriptRunner runner = new BashScriptRunner(complete, tmp);
    /*
     * The last statement in bin/jaqlshell is a stty command. Since there is
     * not a terminal when bin/jaqlshell is launched from a forked Java process
     * for junit test in ANT. This stty command will give a non-zero exit
     * value. So exit value is not checked here.
     */
    runner.run();
  }

  static class RegexReplacement {
    private String regex;
    private String replacement;
    
    public RegexReplacement(String regex, String replacement) {
      this.regex = regex;
      this.replacement = replacement;
    }
    
    public String getRegex() {
      return regex;
    }
    
    public String getReplacement() {
      return replacement;
    }
  }
  
  /**
   * Run a bash script. The stdout and stderr for the script are handled with
   * input stream gobblers in separated threads to avoid hang-up of the script.
   * <p>
   * It is created to run jaqlshell bash script. The stdout and stderr of the
   * script must be handled with a separated thread. So input stream gobller is
   * used. And feeding jaql bash script jaql queries with stdin does not work
   * even though stdin is handled in a separated thread. So jaql queries are fed
   * through input files.
   * 
   * @see InputStreamGobbler
   */
  private static class BashScriptRunner {

    private ProcessBuilder pb;
    private String pathname;

    /**
     * Launches the given script.
     * 
     * @param cmd The bash script and its arguments
     * @param pathname The pathname of the file which collects the content of
     *          stdout.
     * @throws Exception
     */
    public BashScriptRunner(String[] cmd, String pathname) throws Exception {
      this.pathname = pathname;

      String[] executable;
      /*
       * On windows box, a bash script can be launched directly. The bash script
       * must be launched through bash executable.
       */
      if (isWindows) {
        int len = cmd.length;
        executable = new String[len + 1];
        executable[0] = "bash";
        System.arraycopy(cmd, 0, executable, 1, len);
      } else {
        executable = cmd;
      }
      pb = new ProcessBuilder(executable);
      pb.redirectErrorStream(true);
    }

    /**
     * Runs the bash script.
     * 
     * @return exit value after running the bash script
     */
    public int run() throws Exception {
      Process proc = pb.start();
      InputStreamGobbler stdout = new InputStreamGobbler(proc.getInputStream(),
                                                         new PrintStream(new File(pathname)));
      stdout.start();
      proc.waitFor();
      return proc.exitValue();
    }
  }

  /**
   * A input stream gobbler to gobble all the content of the input stream and
   * write this content to a print stream on a separate thread to a print
   * stream.
   */
  private static class InputStreamGobbler extends Thread {
    private InputStream is;
    private OutputStream out;

    /**
     * Constructs a a input stream gobbler.
     * 
     * @param is An input stream
     * @param out An print stream contains all the content from the input stream
     */
    public InputStreamGobbler(InputStream is, OutputStream out) {
      this.is = is;
      this.out = out;
    }

    /**
     * Gobbles the content of the input stream until the end of the input stream
     * is reached. If the input stream is stdout or stderr attached to a
     * process, their ends will be reached after the process is terminated. The
     * output stream will be closed after the end of the input stream is
     * reached.
     */
    @Override
    public void run() {
      try {
        BufferedInputStream bIn = new BufferedInputStream(is);
        byte[] b = new byte[1024];
        int numRead = 0;
        while ((numRead = bIn.read(b, 0, 1024)) > 0) {
          out.write(b, 0, numRead);
          out.flush();
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      } finally {
        try { out.close(); } catch (IOException ioe) { ioe.printStackTrace(); };
      }
    }
  }
}
