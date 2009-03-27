package com.ibm.jaql.util.shell;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Queue;

/** Chains a list of input streams into a new input stream. The new stream will forward reads to
 * the first stream in the chain; input streams that are exhausted are removed from the chain. */

public class ChainedInputStream extends InputStream {
	Queue<InputStream> inputStreams;
	
	public ChainedInputStream() {
		inputStreams = new ArrayDeque<InputStream>();
	}

	public void add(InputStream in) {
		inputStreams.add(in);
	}
	
	@Override
	public int read() throws IOException {
		// check if there an input stream
		if (inputStreams.size() == 0) {
			return -1;
		}
		
		// read from it
		int result = inputStreams.peek().read();
		
		// switch to next stream, if EOS
		while (result < 0 && inputStreams.size()>1) {
 			inputStreams.remove().close(); // remove and close the current input stream
 			result = inputStreams.peek().read(); // and read from the next one
 		}
		if (result<0) {
			inputStreams.remove().close();
		}
		
		return result;
	}
}