package com.ibm.jaql.doc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import com.ibm.jaql.doc.processors.DataProcessor;
import com.ibm.jaql.lang.expr.core.Expr;
import com.sun.javadoc.*;
import com.sun.javadoc.AnnotationDesc.ElementValuePair;

/**
 * This is a special doclet that parses all user defined functions. It is used by the javadoc tool.
 */
public class DocReader {
	private static ArrayList<UDFDesc> functions;
	private static HashMap<String, String> options;
	private static final String UDF_PROCESSORS_OPT = "-udfprocessors";
	private static final String UDF_EXCLUDE_LIST = "-udfexclude";
	private static final String UDF_INCLUDE_LIST = "-udfinclude";
	private static String[] excludes;
	private static String[] includes;
	private static String[] udfProcessorsList = null;

	public static void init(RootDoc root) {
		functions = new ArrayList<UDFDesc>();
		options = new HashMap<String, String>();
		parseOptions(root.options());
	}

	/**
	 * Starts the information collection process. It iterates over all classes and looks for those with
	 * a JaqlFn annotation. Those are then analyzed and put into a list. After all classes are analyzed
	 * the udf processors are called with the function list as argument.
	 * 
	 * @param root 	javadoc documentation root
	 */
	@SuppressWarnings("unchecked")
	public static boolean start(RootDoc root) {
		init(root);

		// Iterate over all classes and find the user defined functions
		UDFDesc function;
		ClassDoc[] classes = root.classes();
		for (ClassDoc c : classes) {
			String cName = c.qualifiedName();
			try {
				Class clazz = Class.forName(cName);
				System.out.println("found class : " + cName);
				if( Expr.class.isAssignableFrom(clazz)) {
					//for (AnnotationDesc ann : c.annotations()) {
					//if (ann.annotationType().name().equals(UDFDesc.FUNCTION_ANNOTATION)) {
					
					// Extract information from annotated function
					function = processUDF(c, clazz);

					// Add function to the function list if it is not excluded
					if (isFunctionIncluded(function.getName())) {
						functions.add(function);
					}
				}
				//}
				//}
			} catch(Exception e) {
				System.err.println("trouble resolving: " + cName);
			}
		}

		// Sort function list
		Collections.sort(functions, new UDFComparator());

		// Execute all udf processors
		ClassLoader cl = DocReader.class.getClassLoader();
		for (String className : udfProcessorsList) {
			try {
				Class<DataProcessor> c = (Class<DataProcessor>) cl.loadClass(className);
				DataProcessor p = c.newInstance();
				p.process(functions, options);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * Processes the given class and extracts the annotation and javadoc
	 * information from it.
	 * 
	 * @param c			information for an function class
	 * @param clazz		the function class
	 * @return a 		object containing the extracted information
	 */
	private static UDFDesc processUDF(ClassDoc c, Class clazz) {
		UDFDesc udf = new UDFDesc(clazz);

		// Get class level javadoc tags
		for (Tag t : c.tags()) {
			udf.addDocumentation(t.name().toUpperCase(), t.text());
		}
		
		// Get javadoc tags from methods (especially eval)
		for (MethodDoc m : c.methods()) {
			for (Tag t : m.tags()) {
				udf.addDocumentation(t.name().toUpperCase(), t.text());
			}
		}

		// If there is no description tag either on class or method level then
		// use the class comment
		if (udf.DESCRIPTION.isEmpty()) {
			udf.addDocumentation(UDFDesc.JAQL_DESCRIPTION_TAG, c.commentText());
		}

		// Add other information
		udf.setClassName(c.qualifiedName());
		return udf;
	}

	/**
	 * Parses the command line options and also puts them into an internal table
	 * so that they are also available to the udf processors.
	 */
	private static void parseOptions(String[][] opts) {
		for (String[] option : opts) {
			if (option.length == 2) {
				options.put(option[0], option[1]);
			}
		}
		udfProcessorsList = options.get(UDF_PROCESSORS_OPT).split(":");
		
		// Get include and exclude command line parameters
		if (options.get(UDF_EXCLUDE_LIST) != null) {
			excludes = options.get(UDF_EXCLUDE_LIST).split(":");
		} else {
			excludes = new String[0];
		}
		if (options.get(UDF_INCLUDE_LIST) != null) {
			includes = options.get(UDF_INCLUDE_LIST).split(":");
		} else {
			includes = new String[0];
		}
	}

	/**
	 * Check if a function is on the include/exclude function list. Exclude is
	 * stronger than include. If the include parameter is not given this function
	 * only checks the exclude list. If both parameters are not give it will
	 * return true.
	 */
	public static boolean isFunctionIncluded(String functionName) {
		boolean include = false;

		// Check the includes list
		if (includes.length > 0) {
			for (int i = 0; i < includes.length; i++) {
				if (includes[i].equals(functionName)) {
					include = true;
				}
			}
		} else {
			include = true;
		}

		// Exclude functions according to exclude list
		for (int i = 0; i < excludes.length; i++) {
			if (excludes[i].equals(functionName)) {
				include = false;
			}
		}
		return include;
	}

	public static int optionLength(String option) {
		if (option.equals("-wikiout")) {
			return 2;
		}
		return 2;
	}
}

/**
 * Comparator class that implements a compare function for UDFDesc objects using
 * the function name.
 */
class UDFComparator implements Comparator<UDFDesc> {

	/**
	 * compare uses the function name. If the function name of the first argument is null the function returns 1
	 * (except both are null than it returns 0). If the function name of the second argument is null the 
	 * function returns -1. Otherwise the upper case function name is used for comparison.
	 */
	@Override
	public int compare(UDFDesc udfa, UDFDesc udfb) {
		if (udfa.getName() == null) {
			if (udfb.getName() == null) {
				return 0;
			} else {
				return 1;
			}
		} else {
			if(udfb.getName() == null) {
				return -1;
			} else {
				return udfa.getName().toUpperCase().compareTo(udfb.getName().toUpperCase());
			}
		}
	}

}
