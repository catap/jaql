/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ibm.jaql.catalog;

import com.ibm.jaql.json.type.*;

public abstract class Catalog {
	
	/**
	 * Opens a catalog connection. The connection configuration is specified in the
	 * catalog connection configuration file.
	 * 
	 * @return Catalog connection
	 */
	abstract public void 
	open() 
	throws CatalogException;
	
	/**
	 * Closes the catalog connection. Underlying resources are released 
	 * 
	 * @throws CatalogException
	 */
	abstract public void
	close();
	
	

	/**
	 * Inserts a entry with the given key.
	 * 
	 * @param key Entry key
	 * @param entry Entry
	 * @param overwrite <code>true</code> to replace the existing entry if an
	 *          entry with the given key already exists; <code>false</code> to
	 *          throw an exception if such an entry already exists.
	 * @throws DuplicatedEntryException If <code>overwrite</code> is set to
	 *           <code>true</code> and an entry with the given key already exists.
	 */
	abstract public void 
	insert( JsonString key, JsonValue entry, boolean overwrite)
	throws CatalogException;

	/**
	 * Deletes the entry specified with the key.
	 * 
	 * @param key Entry key
	 */
	abstract public void
	delete( JsonString key) 
	throws CatalogException;

	/**
	 * Updates the entry under the specified key. If <code>overwrite</code> is
	 * <code>true</code>, this operation has <i>upsert</i> semantics on the record
	 * field level. If an entry with the given key doesn't exist, then it gets
	 * created. If it does exist, then every field in the record gets treated
	 * separately. If the field with the feild name already exists in the old
	 * record, the new value overrides the old. Otherwise, the field just gets
	 * added. For example:
	 * 
	 * <pre>
	 * old catalog entry:  {&quot;foo&quot;:   { &quot;x&quot;: 10, &quot;y&quot;: 20 } } 
	 * command:  update(&quot;foo&quot;,  { &quot;y&quot;:  30, &quot;z&quot;: 40 } ) 
	 * new catalog entry:     {&quot;foo&quot;,   { &quot;x&quot;: 10, &quot;y&quot;:  30 , &quot;z&quot;: 40} }
	 * </pre>
	 * 
	 * If <code>overwrite<code> is <code>false</code>, no old information can be
	 * overwritten. I.e. this operation can only add entry or entry fields.
	 * 
	 * @param key Entry key
	 * @param value Entry
	 * @param overwrite <code>true</code> to use <i>upsert</i> semantics;
	 *          otherwise not.
	 */
	abstract public void 
	update( JsonString key, JsonValue entry, boolean overwrite)
	throws CatalogException;

	/**
	 * Lists all the keys starting with the given prefix.
	 * 
	 * @param keyPrefix Entry key prefix
	 * @return JSON Iterator over all the matched entry keys
	 */
	abstract public JsonArray
	list( JsonString keyPrefix)
	throws CatalogException;

	/**
	 * Returns the entry under the specified key.
	 * 
	 * @param key Entry key
	 * @return The Entry
	 */
	abstract public JsonValue
	get( JsonString key)
	throws CatalogException;

	/**
	 * Returns the field value specified with an entry key and a field name.
	 * 
	 * @param key Entry key
	 * @param fieldName Field name
	 * @return The entry field value
	 */
	abstract public JsonValue
	get( JsonString key, JsonString filedName)
	throws CatalogException;

	/**
	 * Returns the field values specified with a key and field names.
	 * 
	 * @param key Entry key
	 * @param fieldNames Field names
	 * @return The entry field in the format of
	 *         <code>{fieldName1: fieldValue1, fieldName2: fieldName2, ...}</code>
	 */
	abstract public JsonValue
	get( JsonString key, JsonArray filedNames)
	throws CatalogException;
}
