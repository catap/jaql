package com.ibm.jaql.doc;

public class FnParamTag extends FnTag {
	int paramNum = -1;

	@Override
	void setTagData(String text) {
		String[] t = text.trim().split(" ", 2);
		paramNum = Integer.parseInt(t[0]);
		this.text = t[1];
	}

	public int getParamNum() {
		return paramNum;
	}
}
