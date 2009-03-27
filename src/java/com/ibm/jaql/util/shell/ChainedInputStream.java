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