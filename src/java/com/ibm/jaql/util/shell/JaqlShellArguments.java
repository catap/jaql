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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.List;

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
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.JaqlQuery;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.EchoedReader;

/** Parser for arguments of JaqlShell. */
public class JaqlShellArguments {
  static final String DEFAULT_HDFS_DIR = "/tmp/jaql/dfs";
  static final int DEFAULT_NUM_NODES = 1;
  static final boolean DEFAULT_BATCH_MODE = false;
  
  boolean useExistingCluster = false;
  String[] jars = new String[0];
  OutputAdapter outputAdapter;
  String[] searchPath = Module.defaultSearchPath();
  String hdfsDir = DEFAULT_HDFS_DIR;
  int numNodes = DEFAULT_NUM_NODES;
  ChainedReader chainedIn = new ChainedReader();
  boolean batchMode = DEFAULT_BATCH_MODE;
  OutputAdapter logAdapter;

  private JaqlShellArguments() {};
  
  /**
   * Enable or disable the output to console. It is in effect only if running
   * Jaql shell in batch mode with mini clusters. It means to disable console
   * print in batch mode. It is used to disable console print in
   * <code>MiniHBaseCluster</code>, <code>MiniHBaseCluster</code> and
   * <code>MiniDFSCluster</code>. Among these classes,
   * <code>MiniDFSCluster</code> prints startup information to stderr.
   * 
   * @param enable <code>true</code> to enable output to stdout and stderr;
   *          <code>false</code> to disable to output to stdout and stderr.
   * 
   * @see ConsolePrintEnabler
   */
  public void enableConsolePrint(boolean enable) {
    if (batchMode && !useExistingCluster)
      ConsolePrintEnabler.enable(enable);
  }

  /**
   * Adds an input stream to the chained input stream.
   * 
   * @param in An input stream
   */
  private void addInputStream(InputStream in) 
  {
    try
    {
      Reader reader = new InputStreamReader(in, "UTF-8");
      if( !batchMode )
      {
        Writer writer = new OutputStreamWriter(System.out, "UTF-8");
        reader = new EchoedReader(reader, writer);
      }
      chainedIn.add( reader );
    }
    catch( UnsupportedEncodingException e )
    {
      JaqlUtil.rethrow(e);
    }
  }
  
  /**
   * Adds STDIN to the chained input stream.
   */
  private void addStdin()
  {
    try
    {
      chainedIn.add( new InputStreamReader(System.in, "UTF-8") );
    }
    catch( UnsupportedEncodingException e )
    {
      JaqlUtil.rethrow(e);
    }
  }

  @SuppressWarnings("unchecked")
  static JaqlShellArguments parseArgs(String... args) {
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
    
    Option optLog = obuilder
    .withShortName("l")
    .withLongName("log")
    .withDescription("log options: json, del and xml or an output IO descriptor. ")
    .withArgument(abuilder
        .withName("arg")
        .withMinimum(1).withMaximum(1)
        .create())
        .create();

    // combine all options
    Group options = gbuilder
    .withName("options")
    .withOption(optHelp)
    .withOption(optJars)
    .withOption(optSearchPath)
    .withOption(optBatch)
    .withOption(optLog)
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
      printHelpAndExit(e, null, options);
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
          printHelpAndExit("Jar file " + jar + " not found", options);
        }
      }
    }
    
    // search path directories
    if (cl.hasOption(optSearchPath)) {
      result.searchPath = ((String)cl.getValue(optSearchPath)).split(":");
      for (String dir : result.searchPath) {
        if (!new File(dir).exists() || !new File(dir).isDirectory()) {
          printHelpAndExit("Search-path entry " + dir + " not found or is no directory", options);
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
          printHelpAndExit(e,
                          "\"" + format + "\" is neither a valid output format nor a valid IO descriptor",
                          options);
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
          printHelpAndExit(e, "Input file " + file + " not found", options);
        }
      }
    }
    
    // error log
    if (cl.hasOption(optLog)) {
      String path = (String) cl.getValue(optLog);
      try {
        BufferedJsonRecord logFD = new BufferedJsonRecord();
        logFD.add(Adapter.TYPE_NAME, new JsonString("local"));
        logFD.add(Adapter.LOCATION_NAME, new JsonString(path));
        OutputAdapter oa = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(logFD);
        result.logAdapter = oa;
      } catch (Exception e) {
        printHelpAndExit(e,
            "\"" + path + "\" invalid",
            options);
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
    outOptions += ";";
    
    // get output adapter
    JaqlQuery query =  new JaqlQuery();
    query.setQueryString(outOptions);
    JsonRecord options = (JsonRecord) query.evaluate();
    OutputAdapter oa = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(options);
    if (oa == null) 
      throw new NullPointerException();
    else 
      return oa;
  }

  private static void printHelpAndExit(String message, Group options) {
    printHelpAndExit(null, message, options);
  }
  
  @SuppressWarnings("unchecked")
  private static void printHelpAndExit(Exception e, String message, Group options) {
    if (message != null) 
      System.err.println(message);
    if (e != null)
      e.printStackTrace();
    HelpFormatter hf = new HelpFormatter();
    hf.setShellCommand("jaqlshell");
    hf.setGroup(options);
    hf.getFullUsageSettings().remove(DisplaySetting.DISPLAY_GROUP_EXPANDED);
    hf.getLineUsageSettings().add(DisplaySetting.DISPLAY_ARGUMENT_BRACKETED);
    hf.print();
    hf.printHelp();
    System.exit(1);
  }
}