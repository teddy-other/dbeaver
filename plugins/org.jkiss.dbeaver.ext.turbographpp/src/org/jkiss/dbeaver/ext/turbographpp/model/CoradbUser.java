package org.jkiss.dbeaver.ext.turbographpp.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CoradbUser extends GenericSchema {
	
	private final String comment;
    private List<? extends GenericView> edges;
    private List<? extends TurboGraphPPVertex> nodes;

	public CoradbUser(TurboGraphPPDataSource dataSource, String name, String comment) {
		super(dataSource, null, name);
		this.comment = comment;
	}
	
	@Override
	public TurboGraphPPDataSource getDataSource() {
		return (TurboGraphPPDataSource) super.getDataSource();
	}
	
	public String getComment() {
		return comment;
	}

	public List<? extends TurboGraphPPVertex> getVertexs(DBRProgressMonitor monitor) throws DBException {
		if (getDataSource().isNeo4j()) {
			nodes = (List<TurboGraphPPVertex>) super.getPhysicalTables(monitor);
		} else {
			nodes = (List<TurboGraphPPVertex>) loadVertex(monitor);	
		}
    		
    	return nodes == null ? Collections.emptyList() : nodes;
    }
    
    public List<?  extends GenericView> getEdges(DBRProgressMonitor monitor) throws DBException {
        if (edges == null) {
        	if (getDataSource().isNeo4j()) {
        		edges = (List<Neo4jEdge>) loadNeo4jEdges(monitor);
        	} else {
        		edges = (List<TurboGraphPPEdge>) loadEdges(monitor);
        	}
        } 
        return edges == null ? Collections.emptyList() : edges;
    }
    
    private List<? extends TurboGraphPPVertex> loadVertex(DBRProgressMonitor monitor) throws DBException {
        if (nodes != null) {
            return nodes;
        }
        
        List<TurboGraphPPVertex> vertexList = new ArrayList<TurboGraphPPVertex>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
        	StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'VERTEX'");
        	sb.append(" AND owner_name = '").append(this.getName()).append("'");
            try (JDBCPreparedStatement dbStat =
                    session.prepareStatement(sb.toString())) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String class_name = JDBCUtils.safeGetString(dbResult, "class_name");
                        TurboGraphPPVertex vertex = new TurboGraphPPVertex(this.getObject(), class_name, "VERTEX", dbResult);
                        vertexList.add(vertex);
                    }
                    return vertexList;
                }
            }
        } catch (SQLException ex) {
        	throw new DBException("Load Vertex failed", ex);
        }
    }
    
    private List<? extends TurboGraphPPEdge> loadEdges(DBRProgressMonitor monitor) throws DBException {
        if (edges != null) {
            return (List<TurboGraphPPEdge>) edges;
        }
        
        List<TurboGraphPPEdge> edgeList = new ArrayList<TurboGraphPPEdge>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
        	StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'EDGE'");
        	sb.append(" AND owner_name = '").append(this.getName()).append("'");
            try (JDBCPreparedStatement dbStat =
            		session.prepareStatement(sb.toString())) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String class_name = JDBCUtils.safeGetString(dbResult, "class_name");
                        TurboGraphPPEdge edge = new TurboGraphPPEdge(this.getObject(), class_name, "Edge", dbResult);
                        edgeList.add(edge);
                    }
                    return edgeList;
                }
            }
        } catch (SQLException ex) {
        	throw new DBException("Load Edge failed", ex);
        }
    }
    
    private List<? extends Neo4jEdge> loadNeo4jEdges(DBRProgressMonitor monitor) throws DBException {
        if (edges != null) {
            return (List<? extends Neo4jEdge>) edges;
        }
        
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
            try (JDBCPreparedStatement dbStat =
                    session.prepareStatement("CALL db.relationshipTypes()")) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    List<Neo4jEdge> edgeList = new ArrayList<Neo4jEdge>();
                    while (dbResult.next()) {
                        String edgeType = JDBCUtils.safeGetString(dbResult, "relationshipType");
                        Neo4jEdge neo4jEdge = new Neo4jEdge(this.getObject(), edgeType, dbResult);
                        edgeList.add(neo4jEdge);
                    }
                    return edgeList;
                }
            }
        } catch (SQLException ex) {
            throw new DBException("load Neo4j Edge", ex);
        }
    }
}
