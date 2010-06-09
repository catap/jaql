package com.ibm.jaql.util.shell;

public class JaqlShellGetJars {
	public static void main(String[] args) {
		JaqlShellArguments jsa = JaqlShellArguments.parseArgs(args);
		for (String jar : jsa.jars) {
			System.out.println(jar);
		}		
	}
}
