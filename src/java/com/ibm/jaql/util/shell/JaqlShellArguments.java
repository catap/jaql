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

package com.ibm.jaql.util.shell;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import jline.History;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.DisplaySetting;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.commons.cli2.validation.NumberValidator;
import org.apache.commons.lang.StringUtils;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Parser for arguments of JaqlShell. */
public class JaqlShellArguments {
  boolean useExistingCluster = false;
  String[] jars = new String[0];
  OutputAdapter outputAdapter;
  String[] searchPath = Module.defaultSearchPath();
  String hdfsDir = "/tmp/jaql/dfs";
  int numNodes = 1;
  ChainedInputStream chainedIn = new ChainedInputStream();
  boolean batchMode = false;

  private JaqlShellArguments() {};
  
  /**
   * Enable or disable the output to STDOUT and STDOUT. It does nothing in Jaql
   * Shell interactive model. It means to disable console print in batch mode.
   * It is the only way to disable console print in
   * <code>MiniHBaseCluster</code>, <code>MiniHBaseCluster</code> and
   * <code>MiniDFSCluster</code>.
   * 
   * @param enable <code>true</code> to enable output to STDOUT;
   *          <code>false</code> to disable to output to STDOUT.
   */
  public void enableConsolePrint(boolean enable) {
    if (batchMode)
      ConsolePrintEnabler.enable(enable);
  }

  /**
   * Adds an input stream to the chained input stream.
   * 
   * @param in An input stream
   */
  private void addInputStream(InputStream in) {
    chainedIn.add(batchMode ? in : new EchoedInputStream(in));
  }
  
  /**
   * Adds STDIN to the chained input stream.
   */
  private void addStdin() {
    try {
      chainedIn.add(configureConsoleInput());
    } catch (IOException e) {
      chainedIn.add(System.in);
    }
  }
  
  /**
   * Configures a console reader input stream. <tt>.jaql_history</tt> in
   * <tt>user.home</tt> is used to store command history.
   * 
   * @return The console reader input stream
   * @throws IOException
   */
  private ConsoleReaderInputStream configureConsoleInput() throws IOException {
    ConsoleReader cr = new ConsoleReader();
    String historyFile = System.getProperty("user.home") + File.separator
        + ".jaql_history";
    cr.setHistory(new History(new File(historyFile)));
    ConsoleReaderInputStream crIn = new ConsoleReaderInputStream(cr);
    return crIn;
  }
  
  @SuppressWarnings("unchecked")
  static JaqlShellArguments parseArgs(String[] args) {
    // option builders
    final DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    final ArgumentBuilder abuilder = new ArgumentBuilder();
    final GroupBuilder gbuilder = new GroupBuilder();

    // create standard options
    Option optHelp = obuilder
    .withShortName("h")
    .withShortName("?")
    .withLongName("help")
    .withDescription("print this message")
    .create();

    Option optJars = obuilder
    .withShortName("j")
    .withLongName("jars")
    .withDescription("comma-separated list of jar files to include user defined expressions or data stores")
    .withArgument(abuilder
        .withName("args")
        .withMinimum(1).withMaximum(1)
        .create())
        .create();

    Option optSearchPath = obuilder
    .withShortName("jp")
    .withLongName("jaql-path")
    .withDescription("colon seperated list of all search path entries")
    .withArgument(abuilder
        .withName("args")
        .withMinimum(1).withMaximum(1)
        .create())
        .create();
    
    Option optBatch= obuilder
    .withShortName("b")
    .withLongName("batch")
    .withDescription("run in batch mode (i.e., do not read from stdin)")
    .create();
    
    Option optOutOptions = obuilder
    .withShortName("o")
    .withLongName("outoptions")
    .withDescription("output options: json, del and xml or an output IO descriptor. " 
                     + "This option is ignored when not running in batch mode.")
    .withArgument(abuilder
                  .withName("outoptions")
                  .withMinimum(1).withMaximum(1)
                  .create())
                  .create();

    Option optEval = obuilder
    .withShortName("e")
    .withLongName("eval")
    .withDescription("evaluate Jaql expression")
    .withArgument(abuilder
        .withName("expr")
        .withMinimum(1).withMaximum(1)
        .create())
        .create();

    // create mini-cluster options
    Option optCluster = obuilder
    .withShortName("c")
    .withLongName("cluster")
    .withDescription("use existing cluster (i.e., do not launch a mini-cluster)")			
    .create();

    Option optNumNodes= obuilder
    .withShortName("n")	
    .withLongName("no-nodes")								
    .withDescription("mini-cluster option: number of nodes to spawn")
    .withArgument(abuilder
        .withName("arg")
        .withMinimum(1).withMaximum(1)
        .withValidator(NumberValidator.getIntegerInstance())
        .create())
        .create();


    Option optDir = obuilder
    .withShortName("d")
    .withLongName("hdfs-dir")
    .withDescription("mini-cluster option: root HDFs directory")
    .withArgument(abuilder
        .withName("arg")
        .withMinimum(1).withMaximum(1)
        .create())
        .create();

    Group clusterOptions = gbuilder
    .withName("Cluster options")
    .withOption(optCluster)
    .withOption(optDir)
    .withOption(optNumNodes)
    .create();

    // create input files option
    Option optInputFiles = abuilder
    .withName("file")
    .withDescription("list of input files")
    .withMinimum(0)
    .create();

    // combine all options
    Group options = gbuilder
    .withName("options")
    .withOption(optHelp)
    .withOption(optJars)
    .withOption(optSearchPath)
    .withOption(optBatch)
    .withOption(optOutOptions)
    .withOption(optEval)
    .withOption(optInputFiles)
    .withOption(clusterOptions)
    .create();

    // parse and print help if necessary
    CommandLine cl;
    try {
      Parser parser = new Parser();
      parser.setGroup(options);
      cl = parser.parse(args);
    } catch (OptionException e) {
      printHelpAndExit(e.getMessage(), options);
      return null;
    }
    if (cl.hasOption(optHelp)) {
      printHelpAndExit(null, options);
    }

    // validate arguments
    JaqlShellArguments result = new JaqlShellArguments();

    // mini-cluster options			
    if (cl.hasOption(optCluster)) { 
      result.useExistingCluster = true;
    }
    if (cl.hasOption(optDir)) {
      if (result.useExistingCluster) {
        printHelpAndExit("Options " + optCluster.getPreferredName() + " and " 
            + optDir.getPreferredName() + " are mutually exclusive", options);
      }
      result.hdfsDir = (String)cl.getValue(optDir);
    }
    if (cl.hasOption(optNumNodes)) {
      if (result.useExistingCluster) {
        printHelpAndExit("Options " + optCluster.getPreferredName() + " and " 
            + optNumNodes.getPreferredName() + " are mutually exclusive", options);
      }
      result.numNodes = ((Number)cl.getValue(optNumNodes)).intValue();
    }

    // jar files
    if (cl.hasOption(optJars)) {
      result.jars = ((String)cl.getValue(optJars)).split(",");
      for (String jar : result.jars) {
        if (!new File(jar).exists()) {
          printHelp("Jar file " + jar + " not found", options);
          System.exit(1);
        }
      }
    }
    
    // search path directories
    if (cl.hasOption(optSearchPath)) {
      result.searchPath = ((String)cl.getValue(optJars)).split(":");
      for (String dir : result.searchPath) {
        if (!new File(dir).exists() || !new File(dir).isDirectory()) {
          printHelp("Search-path entry " + dir + " not found or is no directory", options);
          System.exit(1);
        }
      }
    }

    if (cl.hasOption(optBatch)) {
      result.batchMode = true;
      if (cl.hasOption(optOutOptions)) {
        String format = (String) cl.getValue(optOutOptions);
        try {
          result.outputAdapter = getOutputAdapter(format);
        } catch (Exception e) {
          printHelpAndExit(format + " is neither a valid output format nor a valid IO descriptor", options);
        }
      } 
    }
    
    // input
    if (cl.hasOption(optEval)) {
      String eval = (String)cl.getValue(optEval);
      if (!eval.endsWith(";")) eval += ";";
      result.addInputStream(new ByteArrayInputStream(eval.getBytes()));
    }
    if (cl.hasOption(optInputFiles)) {
      List<String> files = (List<String>)cl.getValues(optInputFiles);       
      for (String file : files) {
        try {
          result.addInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
          printHelpAndExit("Input file " + file + " not found", options);
        }
      }
    }
    
    if (!result.batchMode) {
        result.addStdin();
    }
    
    return result;
  }

  private static OutputAdapter getOutputAdapter(String outOptions) throws Exception {
    if (outOptions.equals(""))
      outOptions = "jsonStream";
    
    // special casing here, to enable shortcut notation
    if (outOptions.equals("json") || outOptions.equals("del") || outOptions.equals("xml"))
    {
      outOptions += "Stream";
    }
    
    // construct json record for json, del and xml
    if (StringUtils.isAlpha(outOptions)) {
      outOptions = "{" + Adapter.TYPE_NAME + ": '" + outOptions + "'}";
    }

    // get output adapter
    JsonParser jp = new JsonParser(new StringReader(outOptions));
    JsonRecord options = (JsonRecord) jp.TopVal();
    OutputAdapter oa = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(options);
    if (oa == null) 
      throw new NullPointerException();
    else 
      return oa;
  }

  @SuppressWarnings("unchecked")
  static void printHelp(String message, Group options) {
    if (message != null) {
      System.err.println(message);
    }
    HelpFormatter hf = new HelpFormatter();
    hf.setShellCommand("jaqlshell");
    hf.setGroup(options);
    hf.getFullUsageSettings().remove(DisplaySetting.DISPLAY_GROUP_EXPANDED);
    hf.getLineUsageSettings().add(DisplaySetting.DISPLAY_ARGUMENT_BRACKETED);
    hf.print();
    hf.printHelp();
  }
  
  private static void printHelpAndExit(String message, Group options) {
    printHelp(message, options);
    System.exit(1);
  }
  
  private static class EchoedInputStream extends InputStream {
    InputStream in;
    public EchoedInputStream(InputStream in) {
      this.in = in;    
    }
    
    @Override
    public int read() throws IOException
    {
      int b = in.read();
      if (b>0)
      {
        System.out.print((char)b);
      }
      return b;
    }    
  }
}