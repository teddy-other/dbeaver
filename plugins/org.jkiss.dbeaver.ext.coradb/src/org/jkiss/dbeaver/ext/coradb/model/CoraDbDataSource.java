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
package org.jkiss.dbeaver.ext.coradb.model;

import java.sql.SQLException;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.coradb.model.meta.CoraDbMetaModel;
import org.jkiss.dbeaver.ext.coradb.model.plan.CoraDbPlanAnalyser;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceInfo;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

public class CoraDbDataSource extends GenericDataSource {

	private boolean isNeo4j = false;
    private CoradbUserCache userCache;


	public CoraDbDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container, CoraDbMetaModel metaModel,
			CoraDbDialect dialect) throws DBException {
		super(monitor, container, metaModel, dialect);
		if (container.getDriver().getDriverClassName().contains("neo4j")) {
			isNeo4j = true;
        }
		this.userCache = new CoradbUserCache();

	}
	
	@Override
	public CoraDbDataSource getDataSource() {
		return this;
	}
   
    public boolean isNeo4j() {
        return isNeo4j;
    }
    
    public CoradbUserCache getUserCache() {
    	return userCache;
    }
    
    @Override
    protected CoraDbDataSourceInfo createDataSourceInfo(DBRProgressMonitor monitor, @NotNull JDBCDatabaseMetaData metaData) {
        return new CoraDbDataSourceInfo(container.getDriver(), metaData);
    }

    @Override
    public DBSObject refreshObject(DBRProgressMonitor monitor) throws DBException {
        return super.refreshObject(monitor);
    }
    
    public List<CoradbUser> getCoradbUsers(@NotNull DBRProgressMonitor monitor) throws DBException {
        return userCache.getAllObjects(monitor, this);
    }
    
    @Override
    public <T> T getAdapter(Class<T> adapter) {
        if (!this.isNeo4j) {
            if (adapter == DBCQueryPlanner.class) {
                return adapter.cast(new CoraDbPlanAnalyser(this));
            }
        }
        return super.getAdapter(adapter);
    }
    
    public class CoradbUserCache extends JDBCObjectCache<CoraDbDataSource, CoradbUser> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(
                @NotNull JDBCSession session,
                @NotNull CoraDbDataSource container)
                throws SQLException {
        	String sql;
        	if (container.isNeo4j()) {
        		sql = "SHOW HOME DATABASE YIELD name";
        	} else {
        		sql = "select name, comment from db_user";
        	}
            final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
            return dbStat;
        }

        @Nullable
        @Override
        protected CoradbUser fetchObject(
                @NotNull JDBCSession session,
                @NotNull CoraDbDataSource container,
                @NotNull JDBCResultSet dbResult)
                throws SQLException, DBException {
            String name = JDBCUtils.safeGetString(dbResult, "name");
            String comment = "";
            if (!container.isNeo4j) {
            	comment = JDBCUtils.safeGetString(dbResult, "comment");
            }
            return new CoradbUser(container, name, comment);
        }

    }

}
