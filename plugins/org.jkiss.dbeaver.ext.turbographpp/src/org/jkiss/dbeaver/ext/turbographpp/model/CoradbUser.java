package org.jkiss.dbeaver.ext.turbographpp.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CoradbUser extends GenericSchema {
	
	private final String comment;
    private List<TurboGraphPPEdge> edges;
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
    	if (nodes == null) {
    		if (getDataSource().isNeo4j()) {
    			nodes = (List<TurboGraphPPVertex>) super.getPhysicalTables(monitor);
    		} else {
    			nodes = (List<TurboGraphPPVertex>) getPhysicalTables(monitor);	
    		}
    		
    	}
    	return nodes;
    }
    
    public List<?  extends TurboGraphPPEdge> getEdges(DBRProgressMonitor monitor) throws DBException {
        if (edges == null) {
        	if (getDataSource().isNeo4j()) {
        		edges = (List<TurboGraphPPEdge>) super.getViews(monitor);
        	} else {
        		edges = (List<TurboGraphPPEdge>) loadEdges(monitor);
        	}
        } 
        return edges;
    }
    
    private List<? extends TurboGraphPPVertex> loadVertex(DBRProgressMonitor monitor) throws DBException {
        if (nodes != null) {
            return nodes;
        }
        
        List<TurboGraphPPVertex> vertexList = new ArrayList<TurboGraphPPVertex>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
            try (JDBCPreparedStatement dbStat =
                    session.prepareStatement("select * from db_class where class_type = 'VERTEX'")) {
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
            return edges;
        }
        
        List<TurboGraphPPEdge> edgeList = new ArrayList<TurboGraphPPEdge>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
            try (JDBCPreparedStatement dbStat =
            		session.prepareStatement("select * from db_class where class_type = 'EDGE'")) {
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
}
