package com.ibm.jaql.benchmark.programs.data;

public class KeyValue {
	Object key;
	Object value;
	
	public KeyValue(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	public Object getKey() {
		return key;
	}
	public void setKey(Object key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}

}
