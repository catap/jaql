package com.ibm.jaql.util.shell;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

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

/** Parser for arguments of JaqlShell. */
public class JaqlShellArguments {
	boolean useExistingCluster = false;
	String[] jars = new String[0];
	String hdfsDir = "/tmp/jaql/dfs";
	int numNodes = 1;
	InputStream in;
	boolean batchMode = false;

	private JaqlShellArguments() {};
	
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

		Option optBatch= obuilder
		.withShortName("b")
		.withLongName("batch")
		.withDescription("run in batch mode (i.e., do not read from stdin)")
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
  		.withOption(optBatch)
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
			printHelp(e.getMessage(), options);
			System.exit(1);
			return null;
		}
		if (cl.hasOption(optHelp)) {
			printHelp(null, options);
			System.exit(0);
		}
		
		// validate arguments
		JaqlShellArguments result = new JaqlShellArguments();
		
		// mini-cluster options			
		if (cl.hasOption(optCluster)) { 
			result.useExistingCluster = true;
		}
		if (cl.hasOption(optDir)) {
			if (result.useExistingCluster) {
				printHelp("Options " + optCluster.getPreferredName() + " and " 
						+ optDir.getPreferredName() + " are mutually exclusive", options);
				System.exit(1);
			}
			result.hdfsDir = (String)cl.getValue(optDir);
		}
		if (cl.hasOption(optNumNodes)) {
			if (result.useExistingCluster) {
				printHelp("Options " + optCluster.getPreferredName() + " and " 
						+ optNumNodes.getPreferredName() + " are mutually exclusive", options);
				System.exit(1);
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
		
		// input
		ChainedInputStream in = new ChainedInputStream();
		if (cl.hasOption(optEval)) {
			String eval = (String)cl.getValue(optEval);
			if (!eval.endsWith(";")) eval += ";";
			in.add(new ByteArrayInputStream(eval.getBytes()));
		}
		if (cl.hasOption(optInputFiles)) {
			List<String> files = (List<String>)cl.getValues(optInputFiles);				
			for (String file : files) {
				try {
					in.add(new FileInputStream(file));
				} catch (FileNotFoundException e) {
					printHelp("Input file " + file + " not found", options);
					System.exit(1);
				}
			}
		}
		if (cl.hasOption(optBatch)) {
			result.batchMode = true;
		} else {
			// add stdin
			try {
				in.add(new ConsoleReaderInputStream(new ConsoleReader()));
	  	} catch (IOException e) {
	  		in.add(System.in);
	  	}
		}
		result.in = in;
		
		// return result
		return result;
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
}