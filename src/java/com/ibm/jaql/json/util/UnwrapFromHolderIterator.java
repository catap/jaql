package com.ibm.jaql.json.util;

import java.util.Iterator;

import com.ibm.jaql.io.hadoop.JsonHolder;

/** Temporary class to unwrap items */
public final class UnwrapFromHolderIterator extends JsonIterator
{
  Iterator<? extends JsonHolder> iterator;
  
  public UnwrapFromHolderIterator(Iterator<? extends JsonHolder> iterator) {
    this.iterator = iterator;
  }
  
  @Override
protected boolean moveNextRaw() throws Exception
  {
    if (iterator.hasNext()) {
      currentValue = iterator.next().value;
      return true;
    }
    return false;
  }
}
