package com.ibm.jaql.doc.processors;

import java.util.HashMap;
import java.util.List;

import com.ibm.jaql.doc.UDFDesc;

/**
 * Every processor that plugs into the doc read needs to implement this interface. It
 * contains one function that is called to start the processor.
 */
public interface DataProcessor {
	
	/**
	 * Called to start a processor. Data is passed to the processor by using the arguments.
	 * 
	 * @param list 			list of udf functions
	 * @param options		command line options
	 */
	void process(List<UDFDesc> list, HashMap<String, String> options);
}
