package com.ibm.jaql.lang.expr.record;

import java.util.ArrayList;
import java.util.Hashtable;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

enum SortStatus {
	UNDEFINED, UNSTRUCTURED, UNSORTED, ASCENDING, DESCENDING
}

/**
 * This class gets the order information of the attributes included in a given file (or partition)
 * Inputs:
 * 		@param: data: Array of data records 
 * Output:
 * 		@return: JsonArray containing records, one for each attribute (path), the output record format is:
 *           {
 *           	AttrName: <field name>,
 *           	SortingInfo: <one of the values below>
 *           }
 * 			 -UNSTRUCTURED: For attributes that involve nesting
 *           -UNSORTED: For attributes with atom-values and the attribute is not sorted
 *           -ASCENDING: For attributes with atom-values sorted ascending  
 *           -DESCENDING: For attributes with atom-values sorted descending          
 */
public class FieldSortInfo extends IterExpr
{
	//-----Data Members------------------
	private final String ATTR_FIELD_NAME = PathValues.ATTR_FIELD_NAME;
	private final String ATTR_ORDER_NAME = PathValues.ATTR_ORDER_NAME;
    private class FieldInfo {
		SortStatus isSorted;
		SortStatus direction;
	    JsonValue  min_val;
	    JsonValue  max_val;
	};	
	private   ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();
	//-------------------------------------
	
	
    public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
    {
    	public Descriptor()
    	{
    		super("FieldSortInfo", FieldSortInfo.class);
    	}
    }
  
    public FieldSortInfo(Expr[] exprs)
    {
    	super(exprs);
    }

    @Override
    public Schema getSchema()
    {
    	return SchemaFactory.arraySchema();
    }
  

	private   JsonValue get_min_val(int field_index)
	{
		return fields.get(field_index).min_val;
	}

	private   JsonValue get_max_val(int field_index)
	{
		return fields.get(field_index).max_val;
	}

	private   Boolean set_min_val(int field_index, JsonValue field_val)
	{
		try{
			fields.get(field_index).min_val = field_val.getCopy(fields.get(field_index).min_val);
			
		}catch (Exception e)
		{
			System.out.println("Unable to copy JsonValue.");
			return false;
		};
		return true;
	}

	private   Boolean set_max_val(int field_index, JsonValue field_val)
	{
		try{
			fields.get(field_index).max_val = field_val.getCopy(fields.get(field_index).max_val);
		}catch (Exception e)
		{
			System.out.println("Unable to copy JsonValue.");
			return false;
		};
		return true;
	}
	
	/**
	 * Returns true if the new value (new_val) is compatible with the previous values. Otherwise, returns false.
	 */
	private Boolean compatible_dataType(int field_index, JsonValue new_val)
	{
		JsonValue min_val = get_min_val(field_index);
		if (min_val.getType() == new_val.getType())
			return true;
		else if (MathExpr.promote(min_val.getType(), new_val.getType()) != null)
			return true;
		else
			return false;
	}
	
	/**
	 *  Sets the direction of a given attribute (field) as moving ascending or descending 
	 */
	private   Boolean set_direction(int field_index, JsonValue field_val)
	{
		// Before setting the direction, the min&max values are the same
		// If the new value equals the previous value(s), then the direction remains UNDEFINED
		if (field_val.compareTo(get_min_val(field_index)) > 0)
		{
			fields.get(field_index).direction = SortStatus.ASCENDING;
			set_max_val(field_index, field_val);
		}
		else if (field_val.compareTo(get_min_val(field_index)) < 0)
		{
			fields.get(field_index).direction = SortStatus.DESCENDING;
			set_min_val(field_index, field_val);
		}
		return true;
	}
	
	/**
	 * For each field in "fieldNames", return whether the is sorted or not and in which direction (Null values are skipped).
	 */
	private  JsonArray GetOrderInfo(JsonArray dataRecs) throws Exception
	{
		SpilledJsonArray result = new SpilledJsonArray();
		Hashtable<String, Boolean> unique_vals = new Hashtable<String, Boolean>();
		BufferedJsonArray field_names = new BufferedJsonArray();	
		int rec_count = (int) dataRecs.count();
		int field_count = 0;
	
		for (int i = 0; i < rec_count; i++)
		{
			JsonValue v = dataRecs.get(i);
			if (!(v instanceof JsonRecord))
				continue;

			BufferedJsonRecord rec = (BufferedJsonRecord)v;

			//Get the field paths of this record, and add the new ones to field_names array
			BufferedJsonArray rec_paths = JsonRecord.paths(rec.iteratorSorted());
			for (int j = 0; j < rec_paths.count(); j++)
			{
				if (unique_vals.get((rec_paths.get(j)).toString()) == null)
				{
					field_names.add(rec_paths.get(j));
					unique_vals.put((rec_paths.get(j)).toString(), true);
					FieldInfo fi = new FieldInfo();
					fi.direction = SortStatus.UNDEFINED;
					fi.isSorted = SortStatus.UNDEFINED;
					fields.add(fi);
				}
			}
			field_count = (int)field_names.count();

			for (int j = 0; j < field_count; j++)
			{
				if (fields.get(j).isSorted != SortStatus.UNDEFINED)
					continue;
				
				JsonString field_name = (JsonString)field_names.get(j);
				FieldPath fp = FieldPath.fromJsonString(field_name);
				
				if (!fp.canLeafBeSorted())
				{
					fields.get(j).isSorted = SortStatus.UNSTRUCTURED;
					continue;
				}
				
				JsonValue field_val = fp.valueNoArrays(rec);
				if (field_val == null)
					continue;
										
				//If the min&max values are not yet set, then set them
				if (get_min_val(j) == null)
				{
					set_min_val(j, field_val);
					set_max_val(j, field_val);
					continue;
				}

				//If the data type of the values in this filed are not compatible, then mark it as 'UNSTRUCTURED'
				if (!compatible_dataType(j, field_val))
				{
					fields.get(j).isSorted = SortStatus.UNSTRUCTURED;
					continue;
				}
				
				if (fields.get(j).direction == SortStatus.UNDEFINED)
				{
					set_direction(j, field_val);
					continue;
				}

				if ((fields.get(j).direction == SortStatus.ASCENDING && field_val.compareTo(get_max_val(j)) < 0) ||
					(fields.get(j).direction == SortStatus.DESCENDING && field_val.compareTo(get_min_val(j)) > 0))								
				{
					fields.get(j).isSorted = SortStatus.UNSORTED;
					continue;
				}
				else
				{
					if (fields.get(j).direction == SortStatus.ASCENDING) 
						set_max_val(j, field_val); 
					else
						set_min_val(j, field_val);
				}
			}	
		}
		
		//Any field that is marked as UNDEFINED until this point is sorted
		field_count = (int)field_names.count();
		for (int j = 0; j < field_count; j++)
		{
			if (fields.get(j).isSorted == SortStatus.UNDEFINED)
				fields.get(j).isSorted = fields.get(j).direction;
		}		
		
		//Format the output array
		MutableJsonString[] arr = new MutableJsonString[4];
		BufferedJsonRecord s = new BufferedJsonRecord();
		for (int i = 0; i < 4; i++)
			arr[i] = new MutableJsonString();
		
		for (int i = 0; i < field_count; i++)
		{
			s.clear();
			arr[0].setCopy(ATTR_FIELD_NAME);
			arr[1].setCopy(field_names.get(i).toString());
			arr[2].setCopy(ATTR_ORDER_NAME);
			arr[3].setCopy(fields.get(i).isSorted.toString());
			s.add(arr[0], (JsonValue)arr[1]);
			s.add(arr[2], (JsonValue)arr[3]);
			result.addCopy(s);
		}
	    return result;
	}

    
  /**
   * Returns the sorting information for the fields in the given data array.
   * See "src/test/com/ibm/jaql/DataStatistics.txt" for examples.
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray data = (JsonArray) exprs[0].eval(context);
    
    if (data.isEmpty())
    {
      return JsonIterator.EMPTY; 
    }
    
    return (JsonIterator)(GetOrderInfo(data).iterator());
  }
}