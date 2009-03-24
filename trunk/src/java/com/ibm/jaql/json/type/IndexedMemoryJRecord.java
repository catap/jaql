package com.ibm.jaql.json.type;

import java.util.HashMap;
import java.util.Map;

/** Adds hash-based field lookup to MemoryJRecord.
 *
 *  CURRENTLY UNUSED.
 */
public class IndexedMemoryJRecord extends MemoryJRecord {

	protected Map<JString, Integer> indexes;
	
	/**
   * 
   */
  public IndexedMemoryJRecord()
  {
  	super();
  	indexes = new HashMap<JString, Integer>();
  }

  /**
   * @param capacity
   */
  public IndexedMemoryJRecord(int capacity)
  {
  	super(capacity);
  	indexes = new HashMap<JString, Integer>((int)Math.ceil(capacity/0.75f), 0.75f);
  }

  public int findName(JString name)
  {
  	Integer index = indexes.get(name);
  	return index != null ? index : -1;
  }

  public Item getValue(JString name, Item dfault)
  {
  	Integer index = indexes.get(name);
  	return index != null ? values[index] : dfault;
  }
 
  // called when names have change positions
  protected void reorg() {
  	super.reorg();
  	rehash();
  }
  
  private void rehash() {
  	indexes.clear();
  	for (int i=0; i<arity; i++) {
  		indexes.put(names[i], i);
  	}
  }
}
