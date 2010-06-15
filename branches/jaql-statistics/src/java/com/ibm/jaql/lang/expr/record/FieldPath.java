package com.ibm.jaql.lang.expr.record;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

/**
 * Class for extracting the entire root-to-leaf paths (supporting and manipulation functions for paths)  
   * The path conventions are:
   * 	B: B is a field with atom value         "B of type ATOM"
   * 	B.A: B is a record that contains A      "B of type RECORD"
   * 	B[]: B is an array of atom values       "B of type ATOM_ARRAY"
   * 	B[{}]: B is an array of records         "B of type REC_ARRAY"
   * 	B[?]: B is an array mixed types, e.g., B [1, 2, {a:1, b:2}]  "B of type MIX_ARRAY"
   * 	B[[]]: B is an array of arrays                               "B of type ARR_ARRAY"
   *  */
public class FieldPath
{
	public enum Type {UNDEFINED, ATOM, ATOM_ARRAY, REC_ARRAY, ARR_ARRAY, MIX_ARRAY, RECORD};
	
	private JsonString fieldName;
	private Type fieldType;
	private boolean isLeaf;
	private FieldPath childField;

	/**
	 * Creates an empty field path.
	 */
	public FieldPath()
	{
		MutableJsonString s = new MutableJsonString();
		s.setCopy("");
		fieldName = s;
		fieldType = Type.UNDEFINED;
		isLeaf = false;
		childField = null;
	}

	/**
	 * Creates a field path that is a parent to "child" and of type "t".
	 */
	public FieldPath(JsonString name, Type t, boolean leaf, FieldPath child)
	{
		if (leaf) assert (child == null);	
		fieldName = name;
		fieldType = t;
		isLeaf = leaf;
		childField = child;
	}

	/**
	 * Creates a field path of type "t".
	 */
	public FieldPath(JsonString name, Type t, boolean leaf)
	{
		this(name, t, leaf, (FieldPath)null);
	}

	/**
	 * Creates a field path that is a parent to "child" and of type "t".
	 */
	public FieldPath(String name, Type t, boolean leaf, FieldPath child)
	{
		MutableJsonString s = new MutableJsonString();
		s.setCopy(name);
		fieldName = s;
		fieldType = t;
		isLeaf = leaf;
		childField = child;
	}

	public FieldPath(String name, Type t, boolean leaf)
	{
		this(name, t, leaf, (FieldPath)null);
	}

	/** 
	 * Given a JsonString representation of a field path, generate a field path.
	 */
	public static FieldPath fromJsonString(JsonString str_path)
	{
		return fromString(str_path.toString());
	}
	
	/** 
	 * Given a string representation of a field path, generate a field path.
	 */
	public static FieldPath fromString(String str_path)
	{
		boolean crnt_leaf;
		Type    crnt_type;
		String  crnt_name;
		FieldPath fp = new FieldPath();
		
		if (str_path == null)
			return (FieldPath)null;
		else if (str_path.length() <= 0)
			return fp;
		
		String [] path_frgmnts = str_path.split("\\.");
		for( int i = path_frgmnts.length - 1; i >= 0 ; i--)
		{
			crnt_type = Type.UNDEFINED;
			if (i == path_frgmnts.length - 1)
				crnt_leaf = true;
			else
				crnt_leaf = false;

			if (path_frgmnts[i].endsWith("[]"))
			{
				assert (crnt_leaf == true);
				crnt_type = Type.ATOM_ARRAY;
			}
			else if (path_frgmnts[i].endsWith("[[]]"))
			{
				assert (crnt_leaf == true);
				crnt_type = Type.ARR_ARRAY;
			}
			else if (path_frgmnts[i].endsWith("[{}]"))
			{
				assert (crnt_leaf == false);
				crnt_type = Type.REC_ARRAY;
			}
			else if (path_frgmnts[i].endsWith("[?]"))
			{
				assert (crnt_leaf == false);
				crnt_type = Type.MIX_ARRAY;
			}
			
			if (crnt_type == Type.UNDEFINED && crnt_leaf)
				crnt_type = Type.ATOM;
			else if (crnt_type == Type.UNDEFINED && !crnt_leaf)
				crnt_type = Type.RECORD;
			
			crnt_name = path_frgmnts[i].replace("[{}]", "").replace("[[]]", "").replace("[?]", "").replace("[]", "");
			
			if (crnt_leaf)
				fp.set(crnt_name, crnt_type, crnt_leaf, null);
			else
				fp = new FieldPath(crnt_name, crnt_type, crnt_leaf, fp);
		}
		return fp;
	}
	
	/**
	 * Setting the values of the current field path. 
	 */
	public void set(JsonString name, Type t, boolean leaf, FieldPath child)
	{
		fieldName = name;
		fieldType = t;
		isLeaf = leaf;
		childField = child;
	}

	/**
	 * Setting the values of the current field path. 
	 */
	public void set(String name, Type t, boolean leaf, FieldPath child)
	{
		MutableJsonString s = new MutableJsonString();
		s.setCopy(name);
		this.set(s, t, leaf, child);
	}
	
	/**
	 * Returns TRUE if the current entry is leaf   
	 */
	public boolean isLeaf()
	{
		return this.isLeaf;
	}
	
	/**
	 * Returns the type of the current entry in the field path.   
	 */
	public Type getType()
	{
		return this.fieldType;
	}

	/**
	 * Returns the field name of this entry.   
	 */
	public JsonString getName()
	{
		return this.fieldName;
	}

	/**
	 * Returns the child entry of this entry.   
	 */
	public FieldPath getChild()
	{
		return childField;
	}
	
	/**
	 * Sets the child entry of this entry.   
	 */
	public boolean setChild(FieldPath child)
	{
		if (childField != null)
			return false;
		
		childField = child;
		return true;
	}
	
	/**
	 * Returns a string representation of this path.   
	 */
	public String toString()
	{
		if (isLeaf)
		{
			assert (childField == null);
			assert (fieldType == Type.ATOM || fieldType == Type.ATOM_ARRAY || 
					fieldType == Type.ARR_ARRAY || fieldType == Type.MIX_ARRAY);
			if (fieldType == Type.ATOM)
				return fieldName.toString();
			else if (fieldType == Type.ATOM_ARRAY)
				return fieldName.toString() + "[]";
			else if (fieldType == Type.ARR_ARRAY)
				return fieldName.toString() + "[[]]";
			else if (fieldType == Type.MIX_ARRAY)
				return fieldName.toString() + "[?]";
		}
		else
		{
			assert (childField != null);
			assert (fieldType == Type.RECORD || fieldType == Type.REC_ARRAY);
			if (fieldType == Type.RECORD)
				return fieldName.toString() + "." + childField.toString();
			else if (fieldType == Type.REC_ARRAY)
				return fieldName.toString() + "[{}]." + childField.toString();
		}
		return "";
	}

	/**
	 * Returns a JsonString representation of this path.   
	 */
	public JsonString toJsonString()
	{
		MutableJsonString s = new MutableJsonString();
		s.setCopy(this.toString());
		return s;
	}
	
	/**
	 * Returns the length of this path.   
	 */
	public int getLength()
	{
		int len = 1;
		FieldPath fp = this;
		while (fp.getChild() != null)
		{
			len++;
			fp = fp.getChild();
		}
		return len;
	}

	
	/**
	 * Returns the type of the leaf node.   
	 */
	public Type getLeafType()
	{
		FieldPath fp = this;
		while (fp.getChild() != null)
			fp = fp.getChild();

		return fp.getType();
	}

	/**
	 * Adding a parent entry for this path.   
	 */
	public FieldPath addParent(JsonString name, Type t)
	{
		FieldPath fp = new FieldPath(name, t, false, this);
		return fp;
	}

	/**
	 * Returns a string representation of this path that can be used inside Transform expr to return the values of the leaf node.
	 * If the path does not contain arrays, then its transformation is straightforward.
	 * Otherwise, it requires one or more expansions 
	*/
	public String toJsonTransformExpr()
	{
		FieldPath fp = this;
		Type leaf_type = Type.UNDEFINED;
		String s = "", transform_str = "";
		
		if (fp.getType() == Type.UNDEFINED)
			return "";
		
		while (fp != null)
		{
			if (s.equals(""))
				s = fp.getName().toString();
			else	
				s = s + "." + fp.getName().toString();
				
			if (fp.fieldType == Type.REC_ARRAY)
			{
				transform_str = transform_str + " -> transform($." + s + ") -> expand ";
				s = "";
			}
			
			leaf_type = fp.getType();
			fp = fp.getChild();
		}	
		transform_str = transform_str +  " -> transform ($." + s + ") ";

		if (leaf_type == Type.ATOM_ARRAY)
			transform_str = transform_str + " -> expand;";
		else
			transform_str = transform_str + ";";
		
		return transform_str;
	}	


	/**
	 * Does this path contain arrays (except for the leaf).
	 * @return
	 */
	public boolean containArrays()
	{
		FieldPath fp = this;
		while (fp != null)
		{
			if ((fp.getType() == Type.ARR_ARRAY) ||	(fp.getType() == Type.MIX_ARRAY) || 
					(fp.getType() == Type.REC_ARRAY))
				return true;

			fp = fp.getChild();
		}
		return false;
	}

	/**
	 * Values at the leaf of a given path have the potential to be sorted only if this path does not contain arrays
	 * @return
	 */
	public boolean canLeafBeSorted()
	{
		FieldPath fp = this;
		while (fp != null)
		{
			if ((fp.getType() == Type.ARR_ARRAY) || (fp.getType() == Type.ATOM_ARRAY) || 
					(fp.getType() == Type.MIX_ARRAY) || (fp.getType() == Type.REC_ARRAY))
				return false;

			fp = fp.getChild();
		}
		return true;
	}

	/**
	 * Return the value of the leaf entry in the given record. 
	 * If the path contains arrays, the returned value is NULL.
	 */
	public JsonValue valueNoArrays(JsonRecord rec)
	{
		FieldPath fp = this;
		JsonValue field_val;
		
		if (!canLeafBeSorted())
			return (JsonValue)null;

		field_val = rec.get(fp.getName());
		if (field_val == null)
			return (JsonValue)null;
		
		while (fp.getChild() != null)
		{
			fp = fp.getChild();
			field_val = ((BufferedJsonRecord)field_val).get(fp.getName());
			if (field_val == null)
				return (JsonValue)null;		
		}
		return field_val;
	}
	
	/**
	 * Return the value of the leaf entry in the given record. 
	 * If the path contains arrays, the returned value is NULL.
	 * The returned value is either atomic or array of atomic values.
	 */
	public JsonValue atomicValues(JsonRecord rec)
	{
		FieldPath fp = this;
		JsonValue field_val;
		
		if (containArrays())
			return (JsonValue)null;

		field_val = rec.get(fp.getName());
		if (field_val == null)
			return (JsonValue)null;
		
		while (fp.getChild() != null)
		{
			fp = fp.getChild();
			field_val = ((BufferedJsonRecord)field_val).get(fp.getName());
			if (field_val == null)
				return (JsonValue)null;		
		}
		return field_val;
	}
}