package com.ibm.jaql.benchmark.programs.data.generator;

import java.util.Random;

import com.ibm.jaql.benchmark.DataGenerator;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;

public class TransitionGenerator implements DataGenerator {

	private static final JsonString mcc1 = new JsonString("MCC1");
	private static final JsonString mcc2 = new JsonString("MCC2");
	private static final JsonString zip1 = new JsonString("ZIP1");
	private static final JsonString zip2 = new JsonString("ZIP2");
	private Random rnd = new Random(1988);
	
	@Override
	public BufferedJsonRecord generate() {
		BufferedJsonRecord rec = new BufferedJsonRecord(4);
		if(rnd.nextBoolean()) {
			rec.add(mcc1, new JsonLong(rnd.nextLong()%9999));
		}
		if(rnd.nextBoolean()) {
			rec.add(zip1, new JsonLong(rnd.nextLong()%999990000));
		}
		rec.add(mcc2, new JsonLong(rnd.nextLong()%999990000));
		rec.add(zip2, new JsonLong(rnd.nextLong()%999990000));
		return rec;
	}

}
