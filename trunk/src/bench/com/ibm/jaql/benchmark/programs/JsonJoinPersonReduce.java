package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.FieldNameCache;
import com.ibm.jaql.json.util.JsonIterator;

public class JsonJoinPersonReduce extends JsonBenchmarkProgramSingleInput {

	private static final JsonString F = FieldNameCache.get(new JsonString("f"));
	private static final JsonString L = FieldNameCache.get(new JsonString("l"));
	private static final JsonString ID = FieldNameCache.get(new JsonString("id"));
	private static final JsonString ID_A = FieldNameCache.get(new JsonString("idA"));
	private static final JsonString ID_B = FieldNameCache.get(new JsonString("idB"));
	private SpilledJsonArray results = new SpilledJsonArray();
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		try {
			JsonArray input = (JsonArray) val;
			JsonIterator iter = input.iter();
			iter.moveNext();
			//TODO: Don't read joinKey -> investigate wether joinkey needs ot be deserialized
			//when not used and when iterated over but not requested
			JsonValue joinKey = iter.current();
			iter.moveNext();
			JsonArray valuesA = (JsonArray) iter.current();
			iter.moveNext();
			JsonArray valuesB = (JsonArray) iter.current();
			
			return join(joinKey, valuesA, valuesB);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	private JsonValue join(JsonValue joinKey, JsonArray valuesA, JsonArray valuesB) throws Exception{
		results.clear();
		if(joinKey != null) {
			JsonIterator iterA = valuesA.iter();
			while(iterA.moveNext()) {
				JsonRecord recA = (JsonRecord) iterA.current();
				JsonValue idA = ((JsonRecord)recA.get(F)).get(ID);
				JsonIterator iterB = valuesB.iter();
				while(iterB.moveNext()) {
					JsonRecord recB = (JsonRecord) iterB.current();
					BufferedJsonRecord result = new BufferedJsonRecord(2);
					JsonString[] names = new JsonString[] { ID_A, ID_B };
					JsonValue[] vals = new JsonValue[] {
							idA,
							((JsonRecord)recB.get(L)).get(ID)
					};
					result.set(names, vals, 2);
					results.add(result);
				}
			}
		}
		results.freeze();
		return results;
	}

}
