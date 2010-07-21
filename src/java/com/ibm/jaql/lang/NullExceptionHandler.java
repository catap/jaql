package com.ibm.jaql.lang;

import com.ibm.jaql.json.type.JsonValue;

public class NullExceptionHandler extends ExceptionHandler{

	@Override
	public void handleException(Throwable error, JsonValue ctx)
			throws Exception {
		// do nothing
	}

}
