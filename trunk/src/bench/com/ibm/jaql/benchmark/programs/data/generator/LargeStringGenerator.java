package com.ibm.jaql.benchmark.programs.data.generator;

import java.util.Random;

import com.ibm.jaql.benchmark.DataGenerator;
import com.ibm.jaql.json.type.JsonString;

public class LargeStringGenerator implements DataGenerator {

	private char[] characters = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
			'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
	private Random rnd = new Random(1988);
	
	@Override
	public JsonString generate() {
		int size = 100 + rnd.nextInt(400);
		char[] strCharacters = new char[size];
		for (int i = 0; i < strCharacters.length; i++) {
			strCharacters[i] = characters[rnd.nextInt(characters.length)];
		}
		String s = new String(strCharacters);
		
		return new JsonString(s);
	}

}
