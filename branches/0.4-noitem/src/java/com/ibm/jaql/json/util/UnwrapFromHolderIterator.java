package com.ibm.jaql.json.util;

import java.util.Iterator;

import com.ibm.jaql.io.hadoop.JsonHolder;

/** Temporary class to unwrap items */
public final class UnwrapFromHolderIterator extends JsonIterator
{
  Iterator<JsonHolder> iterator;
  
  public UnwrapFromHolderIterator(Iterator<JsonHolder> iterator) {
    this.iterator = iterator;
  }
  
  @Override
  public boolean moveNext() throws Exception
  {
    if (iterator.hasNext()) {
      currentValue = iterator.next().value;
      return true;
    }
    return false;
  }
}
