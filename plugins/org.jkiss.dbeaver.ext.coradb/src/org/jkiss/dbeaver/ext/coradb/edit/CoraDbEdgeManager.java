/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.coradb.edit;

import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.edit.GenericTableManager;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.coradb.model.CoraDbEdge;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CoraDbEdgeManager extends GenericTableManager 
{

    @Override
    public boolean canCreateObject(Object container) {
        return false;
    }
    
    @Override
    public boolean canDeleteObject(GenericTableBase object) {
        return false;
    }
    
    @Override
    public boolean canEditObject(GenericTableBase object) {
        return false;
    }
    
    @Override
    protected void appendTableModifiers(DBRProgressMonitor monitor, GenericTableBase table,
    		NestedObjectCommand tableProps, StringBuilder ddl, boolean alter, Map<String, Object> options)
    		throws DBException {
    	if (table instanceof CoraDbEdge) {
    		String target = "CREATE TABLE";
            String replacement = "CREATE EDGE TABLE";
            
            int startIndex = ddl.indexOf(target);

            if (startIndex != -1) {
                int endIndex = startIndex + target.length();
                ddl.replace(startIndex, endIndex, replacement);
            }
    	}
    }
}
