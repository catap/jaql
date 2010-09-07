package com.ibm.jaql.benchmark.util;

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

import com.ibm.jaql.util.shell.JaqlShellArguments;

public class BenchmarkShellArguments {
	public static String type;
	public static String[] typeArgs = { "jaql", "json", "java", "hadoop-read", "hadoop-write", "raw-write", "raw-read" };
	public static String benchmark;
	public static String serializer;
	public static String[] serializerArgs = { "generated", "hdfs", "jaqltemp", "perf" };
	public static String filesystem;
	public static String[] filesystemArgs = { "memory", "local" };
	public static String modePostfix;

	public static JaqlShellArguments parseArgs(String... args) {
		// option builders
		final DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
		final ArgumentBuilder abuilder = new ArgumentBuilder();
		final GroupBuilder gbuilder = new GroupBuilder();

		// create standard options
		Option optHelp = obuilder.withShortName("h").withShortName("?")
				.withLongName("help").withDescription("print this message")
				.create();

		//Replace by mode
		Option optType = obuilder
				.withShortName("t")
				.withLongName("type")
				.withDescription(
						"Which type of benchmark should be run " + listOptions(typeArgs))
				.withRequired(true).withArgument(
						abuilder.withName("args").withMinimum(1).withMaximum(1)
								.create()).create();

		Option optBenchmark = obuilder
				.withShortName("b")
				.withLongName("benchmark")
				.withDescription("The benchmark program to run")
				.withRequired(true).withArgument(
						abuilder.withName("args").withMinimum(1).withMaximum(1)
								.create()).create();

		Option optSerializer = obuilder
				.withShortName("s")
				.withLongName("serializer")
				.withDescription("Serializer selection " + listOptions(serializerArgs))
				.withArgument(
						abuilder.withName("args").withMinimum(1).withMaximum(1)
								.create()).create();
		
		Option optFilesystem = obuilder
				.withShortName("f")
				.withLongName("filesystem")
				.withDescription("Filesystem selection " + listOptions(filesystemArgs))
				.withArgument(
						abuilder.withName("args").withMinimum(1).withMaximum(1)
								.create()).create();
		
		Option optModePostfix = obuilder
		.withShortName("p")
		.withLongName("postfix")
		.withDescription("Postfix that is added to the mode (useful when testing different jaql versions)")
		.withArgument(
				abuilder.withName("args").withMinimum(1).withMaximum(1)
						.create()).create();

		// combine all options
		Group options = gbuilder.withName("options").withOption(optHelp)
				.withOption(optBenchmark).withOption(optType).withOption(
						optSerializer).withOption(optFilesystem).withOption(optModePostfix).create();

		// TODO: Option for pretty print
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

		// type
		type = (String) cl.getValue(optType);
		if(!checkArgument(type, typeArgs)) {
			printHelpAndExit("Not a valid argument for type", options);
		}
		// benchmark
		benchmark = (String) cl.getValue(optBenchmark);
		try {
			BenchmarkConfig.getBenchmarkRecord(benchmark);
		} catch (Exception e) {
			printHelpAndExit(e, "Benchmark could not be opened", options);
		}

		// serializer
		if (cl.hasOption(optSerializer)) {
			serializer = (String) cl.getValue(optSerializer);
			if(!checkArgument(serializer, serializerArgs)) {
				printHelpAndExit("Not a valid argument for serializer", options);
			}
			if("java".equalsIgnoreCase(type)) {
				printHelpAndExit("Cannot set serializer for java", options);
			}
		}
		
		// filesystem
		if (cl.hasOption(optFilesystem)) {
			filesystem = (String) cl.getValue(optFilesystem);
			if(!checkArgument(filesystem, filesystemArgs)) {
				printHelpAndExit("Not a valid argument for filesystem", options);
			}
			if("java".equalsIgnoreCase(type)) {
				printHelpAndExit("Cannot set filesystem for java", options);
			}
		}
		
		if (cl.hasOption(optModePostfix)) {
			modePostfix = (String) cl.getValue(optModePostfix);
		}
		
		//TODO: Do constant strings as static variables
		if("fs".equalsIgnoreCase(type) && serializer == null) {
			printHelpAndExit("When type 'fs' is selected the fileSystemoption needs to be set", options);
		}


		return null;
	}
	
	private static String listOptions(String[] args) {
		String s = "(";
		for (int i = 0; i < args.length; i++) {
			if(i > 0) {
				s+=", ";
			}
			s += args[i]; 
		}
		
		s += ")";
		return s;
	}

	private static boolean checkArgument(String arg, String[] valid) {
		for (int i = 0; i < valid.length; i++) {
			if(valid[i].equalsIgnoreCase(arg)) {
				return true;
			}
		}
		return false;
	}

	private static void printHelpAndExit(String message, Group options) {
		printHelpAndExit(null, message, options);
	}

	@SuppressWarnings("unchecked")
	private static void printHelpAndExit(Exception e, String message,
			Group options) {
		if (message != null)
			System.err.println(message);
		if (e != null)
			e.printStackTrace();
		HelpFormatter hf = new HelpFormatter();
		hf.setShellCommand("benchmark");
		hf.setGroup(options);
		hf.getFullUsageSettings().remove(DisplaySetting.DISPLAY_GROUP_EXPANDED);
		hf.getLineUsageSettings()
				.add(DisplaySetting.DISPLAY_ARGUMENT_BRACKETED);
		hf.print();
		hf.printHelp();
		System.exit(1);
	}

}
