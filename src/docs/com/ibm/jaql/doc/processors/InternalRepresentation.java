package com.ibm.jaql.doc.processors;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;

import com.ibm.jaql.doc.FnTextTag;
import com.ibm.jaql.doc.UDFDesc;

public class InternalRepresentation implements DataProcessor {
	String intFile;
	@Override
	public void process(List<UDFDesc> list, HashMap<String, String> options) {
		intFile = options.get("-intfile");
		File f = openOutputFile();
		PrintStream srcWriter = null;
		try {
			srcWriter = new PrintStream(f);
		} catch (IOException e) {
			throw new RuntimeException(FunctionListProcessor.class
					+ " could not create function list class file", e);
		}
		
		for (UDFDesc desc : list) {
			try {
				srcWriter.print(""+URLEncoder.encode(desc.getName(), "UTF-8"));
				srcWriter.print(" Min:" + URLEncoder.encode(desc.getMinArgs()+"", "UTF-8"));
				srcWriter.print(" Max:" + URLEncoder.encode(desc.getMaxArgs()+"", "UTF-8"));
				for (FnTextTag tag : desc.DESCRIPTION) {
					srcWriter.print(" Description:" + URLEncoder.encode(tag.getText()+"", "UTF-8"));
				}
				for (FnTextTag tag : desc.EXAMPLES) {
					srcWriter.print(" Example:" + URLEncoder.encode(tag.getText()+"", "UTF-8"));
				}
				/*for (FnTextTag tag : desc.RETURN.getTagData()) {
					srcWriter.print(" Return:" + URLEncoder.encode(tag.getText()+"", "UTF-8"));
				}*/
				srcWriter.println();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			
		}

	}
	
	File openOutputFile() {
		String filePath = intFile;
		File f = new File(filePath);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!f.canWrite()) {
			throw new RuntimeException("Cannot write to file " + filePath);
		}
		return f;
	}
}
