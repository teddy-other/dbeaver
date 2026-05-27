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
package org.jkiss.dbeaver.ext.turbographpp.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBDatabaseException;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSourceInfo;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.turbographpp.model.meta.TurboGraphPPMetaModel;
import org.jkiss.dbeaver.ext.turbographpp.model.plan.TurboGraphPPPlanAnalyser;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceInfo;
import org.jkiss.dbeaver.model.DBUtils;
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

public class TurboGraphPPDataSource extends GenericDataSource {

	private boolean isNeo4j = false;
    private DBPDataSourceInfo dataSourceInfo;
    private List<TurboGraphPPEdge> edges;
    private List<? extends TurboGraphPPVertex> nodes;
    private CoradbUserCache userCache;


	public TurboGraphPPDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container, TurboGraphPPMetaModel metaModel,
			TurboPPSQLDialect dialect) throws DBException {
		super(monitor, container, metaModel, dialect);
		if (container.getDriver().getDriverClassName().contains("neo4j")) {
			isNeo4j = true;
        }
		this.userCache = new CoradbUserCache();

	}
	
	@Override
	public TurboGraphPPDataSource getDataSource() {
		return this;
	}
   
    public boolean isNeo4j() {
        return isNeo4j;
    }

    @Override
    protected DBPDataSourceInfo createDataSourceInfo(DBRProgressMonitor monitor, @NotNull JDBCDatabaseMetaData metaData) {
        return new TurboGraphPPDataSourceInfo(container.getDriver(), metaData);
    }

    @Override
    public DBSObject refreshObject(DBRProgressMonitor monitor) throws DBException {
        this.edges = null;
        return super.refreshObject(monitor);
    }
    
    public List<CoradbUser> getCoradbUsers(@NotNull DBRProgressMonitor monitor) throws DBException {
        return userCache.getAllObjects(monitor, this);
    }
    
//    public List<? extends TurboGraphPPVertex> getVertexs(DBRProgressMonitor monitor) throws DBException {
//    	if (nodes == null) {
//    		nodes = (List<TurboGraphPPVertex>) loadVertex(monitor);
//    	}
//    	return nodes;
//    }
//    
//    public List<?  extends TurboGraphPPEdge> getEdges(DBRProgressMonitor monitor) throws DBException {
//        if (edges == null) {
//            edges = (List<TurboGraphPPEdge>) loadEdges(monitor);
//        }
//        return edges;
//    }
//    
//    private TurboGraphPPEdge getEdge(DBRProgressMonitor monitor, String edgeName) throws DBException {
//        if (edges != null) {
//            Iterator itr = edges.iterator();
//            while(itr.hasNext()) {
//                TurboGraphPPEdge edge = (TurboGraphPPEdge) itr.next();
//                if (edge.getName().equals(edgeName)) {
//                    return edge;
//                }
//            }
//        }
//        return null;
//    }
//    
//    private List<? extends TurboGraphPPVertex> loadVertex(DBRProgressMonitor monitor) throws DBException {
//        if (nodes != null) {
//            return nodes;
//        }
//        
//        List<TurboGraphPPVertex> vertexList = new ArrayList<TurboGraphPPVertex>();
//        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
//            try (JDBCPreparedStatement dbStat =
//                    session.prepareStatement("select * from db_class where class_type = 'VERTEX'")) {
//                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
//                    while (dbResult.next()) {
//                        String class_name = JDBCUtils.safeGetString(dbResult, "class_name");
//                        TurboGraphPPVertex vertex = new TurboGraphPPVertex(this.getObject(), class_name, "", dbResult);
//                        vertexList.add(vertex);
//                    }
//                    return vertexList;
//                }
//            }
//        } catch (SQLException ex) {
//            throw new DBDatabaseException(ex, this);
//        }
//    }
//    
//    private List<? extends TurboGraphPPEdge> loadEdges(DBRProgressMonitor monitor) throws DBException {
//        if (edges != null) {
//            return edges;
//        }
//        
//        List<Neo4jEdge> edgeList = new ArrayList<Neo4jEdge>();
//        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
//            try (JDBCPreparedStatement dbStat =
//            		session.prepareStatement("select * from db_class where class_type = 'EDGE'")) {
//                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
//                    while (dbResult.next()) {
//                        String edgeType = JDBCUtils.safeGetString(dbResult, "relationshipType");
//                        Neo4jEdge neo4jEdge = new Neo4jEdge(this.getObject(), edgeType, dbResult);
//                        edgeList.add(neo4jEdge);
//                    }
//                    return edgeList;
//                }
//            }
//        } catch (SQLException ex) {
//            throw new DBDatabaseException(ex, this);
//        }
//    }
//    
//    @Override
//    public List<? extends GenericView> getViews(DBRProgressMonitor monitor) throws DBException {
//        if (this.isNeo4j) {
//            return getEdges(monitor);
//        }
//        return super.getViews(monitor);
//    }
    
//    @Override
//    public Collection<? extends DBSObject> getChildren(DBRProgressMonitor monitor)
//            throws DBException {
//        List<Object> ret = new ArrayList<Object>();
//        ret.addAll(super.getChildren(monitor));
//        if (this.isNeo4j) {
//            ret.addAll(getEdges(monitor));
//        }
//        return (Collection<? extends DBSObject>) ret;
//    }
    
//    @Override
//    public DBSObject getChild(DBRProgressMonitor monitor, String childName) throws DBException {
//        if (this.isNeo4j) {
//            DBSObject obj = (DBSObject) this.getEdge(monitor, childName);
//            if (obj == null) {
//                obj = super.getChild(monitor, childName);
//            }
//            return obj;
//        } 
//        return super.getChild(monitor,childName);
//    } 

    @Override
    public <T> T getAdapter(Class<T> adapter) {
        if (!this.isNeo4j) {
            if (adapter == DBCQueryPlanner.class) {
                return adapter.cast(new TurboGraphPPPlanAnalyser(this));
            }
        }
        return super.getAdapter(adapter);
    }
    
    public class CoradbUserCache extends JDBCObjectCache<TurboGraphPPDataSource, CoradbUser> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(
                @NotNull JDBCSession session,
                @NotNull TurboGraphPPDataSource container)
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
                @NotNull TurboGraphPPDataSource container,
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
