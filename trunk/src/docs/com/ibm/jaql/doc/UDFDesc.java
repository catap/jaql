package com.ibm.jaql.doc;

import java.lang.reflect.Field;
import java.util.HashMap;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/**
 * A class to collect all the information for a specific user defined function.
 * This includes the information provided by the annotations and information in
 * the javadocs.
 */
public class UDFDesc {
	static final String JAQL_DESCRIPTION_TAG = "@jaqlDescription".toUpperCase();
	static final String JAQL_EXAMPLE_TAG = "@jaqlExample".toUpperCase();
	//static final String FUNCTION_ANNOTATION = "JaqlFn";
	//static final String ANN_FNNAME = "fnName";
	//static final String ANN_MINARGS = "minArgs";
	//static final String ANN_MAXARGS = "maxArgs";
	//static final String[] ANN_ATTRIBS = { ANN_FNNAME, ANN_MINARGS, ANN_MAXARGS };

	private HashMap<String, String> annotation;
	private HashMap<String, FnTagList<? extends FnTag>> tagMapping;
	private String fullyQualifiedClassName;
	private BuiltInFunctionDescriptor descriptor;

	public final FnTagList<FnTextTag> DESCRIPTION = new FnTagList<FnTextTag>(FnTextTag.class);
	public final FnTagList<FnTextTag> EXAMPLES = new FnTagList<FnTextTag>(FnTextTag.class);

	public UDFDesc(Class clazz) {
		
		try {
			Class[] cs = clazz.getDeclaredClasses();
			for(int i = 0; i < cs.length; i++) {
				if( BuiltInFunctionDescriptor.class.isAssignableFrom(cs[i]) ) {
					descriptor = (BuiltInFunctionDescriptor)cs[i].newInstance();
				}
			}
			//Field df = clazz.getField("Descriptor");
			//descriptor = (BuiltInFunctionDescriptor)df.getDeclaringClass().newInstance();
		} catch(Exception e) {
			e.printStackTrace();
		}
		annotation = new HashMap<String, String>();
		tagMapping = new HashMap<String, FnTagList<? extends FnTag>>();
		tagMapping.put(JAQL_DESCRIPTION_TAG, DESCRIPTION);
		tagMapping.put(JAQL_EXAMPLE_TAG, EXAMPLES);
	}

	/**
	 * Adds annotation information to this udf
	 * 
	 * @param name
	 *          name of the annotation field
	 * @param value
	 *          value of the annotation field
	 */
	void setAnnotationAttrib(String name, String value) {
		annotation.put(name, value);
	}

	/**
	 * Adds information provided by javadoc comments to this udf
	 * 
	 * @param tag
	 *          the tag name
	 * @param comment
	 *          the tag comment
	 */
	void addDocumentation(String tag, String comment) {
		FnTagList<? extends FnTag> docTag = tagMapping.get(tag);
		if (docTag != null) {
			docTag.add(comment);
		}
	}

	void setClassName(String qualifiedName) {
		fullyQualifiedClassName = qualifiedName;
	}

	public String getName() {
		//String ann = annotation.get(ANN_FNNAME);
		//return ann.substring(1, ann.length() - 1);
		return descriptor.getName();
	}

	public int getMinArgs() {
		//return Integer.parseInt(annotation.get(ANN_MINARGS));
		return descriptor.getParameters().numRequiredParameters();
	}

	public int getMaxArgs() {
		//return Integer.parseInt(annotation.get(ANN_MAXARGS));
		return descriptor.getParameters().numParameters();
	}
	
	public Schema getReturnSchema() {
		return descriptor.getSchema();
	}
	
	public String getArgInfo() {
		StringBuilder sb = new StringBuilder();
		JsonValueParameters params = descriptor.getParameters();
		int n = params.numParameters();
		for(int i = 0; i < n; i++) {
			Schema s = params.schemaOf(i);
			JsonValue dv = params.defaultOf(i);
			boolean bv = params.hasDefault(i);
			boolean br = params.isRequired(i);
			JsonString name = params.nameOf(i);
			
			try {
			if(br)
				sb.append("[");
			sb.append(s.toString());
			if( name == null )
				sb.append(" " + i);
			else
				sb.append(" " + name);
			if( bv )
				sb.append(" = " + JsonUtil.printToString(dv));
			if(br) 
				sb.append("]");
			if( (i + 1) < i)
				sb.append(",");
			} catch(Exception e) {
				return "";
			}
		}
		if( params.hasRepeatingParameter() )
			sb.append("...");
		
		return sb.toString();
	}

	public String getClassName() {
		int lastDotPos = fullyQualifiedClassName.lastIndexOf('.');
		return fullyQualifiedClassName.substring(lastDotPos + 1);
	}

	public String getFullyQualifiedClassName() {
		return fullyQualifiedClassName;
	}

	public String getPackageName() {
		int lastDotPos = fullyQualifiedClassName.lastIndexOf('.');
		return fullyQualifiedClassName.substring(0, lastDotPos);
	}

	public String toString() {
		//return annotation.toString();
		return descriptor.toString();
	}
}
