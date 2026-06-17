package org.jkiss.dbeaver.ext.turbographpp.model;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.TableCache;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CoradbUser extends GenericSchema {
	
	private final String comment;
    private List<? extends GenericView> edges;
    private List<? extends CoraDbVertex> nodes;
    private CoraDbVertexCache coraDbVertexCache;
    private CoraDbEdgeCache coraDbEdgeCache;

	public CoradbUser(CoraDbDataSource dataSource, String name, String comment) {
		super(dataSource, null, name);
		this.comment = comment;
		coraDbVertexCache = new CoraDbVertexCache();
		coraDbEdgeCache = new CoraDbEdgeCache();
	}
	
	@Override
	public CoraDbDataSource getDataSource() {
		return (CoraDbDataSource) super.getDataSource();
	}
	
	public String getComment() {
		return comment;
	}
	
	public List<? extends CoraDbVertex> getVertexs(DBRProgressMonitor monitor) throws DBException {
		List<? extends CoraDbVertex> tables = coraDbVertexCache.getAllObjects(monitor, this);
		return tables;
    }
    
    public List<?  extends CoraDbEdge> getEdges(DBRProgressMonitor monitor) throws DBException {
    	List<?  extends CoraDbEdge> tables = coraDbEdgeCache.getAllObjects(monitor, this);
    	return tables;
    }
    
//    private List<? extends CoraDbVertex> loadVertex(DBRProgressMonitor monitor) throws DBException {
//        if (nodes != null) {
//            return nodes;
//        }
//        
//        List<CoraDbVertex> vertexList = new ArrayList<CoraDbVertex>();
//        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
//        	StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'VERTEX'");
//        	sb.append(" AND owner_name = '").append(this.getName()).append("'");
//            try (JDBCPreparedStatement dbStat =
//                    session.prepareStatement(sb.toString())) {
//                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
//                    while (dbResult.next()) {
//                        String class_name = JDBCUtils.safeGetString(dbResult, "class_name");
//                        CoraDbVertex vertex = new CoraDbVertex(this.getObject(), class_name, "VERTEX", dbResult);
//                        vertexList.add(vertex);
//                    }
//                    return vertexList;
//                }
//            }
//        } catch (SQLException ex) {
//        	throw new DBException("Load Vertex failed", ex);
//        }
//    }
//    
//    private List<? extends CoraDbEdge> loadEdges(DBRProgressMonitor monitor) throws DBException {
//        if (edges != null) {
//            return (List<CoraDbEdge>) edges;
//        }
//        
//        List<CoraDbEdge> edgeList = new ArrayList<CoraDbEdge>();
//        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
//        	StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'EDGE'");
//        	sb.append(" AND owner_name = '").append(this.getName()).append("'");
//            try (JDBCPreparedStatement dbStat =
//            		session.prepareStatement(sb.toString())) {
//                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
//                    while (dbResult.next()) {
//                        String class_name = JDBCUtils.safeGetString(dbResult, "class_name");
//                        CoraDbEdge edge = new CoraDbEdge(this.getObject(), class_name, "Edge", dbResult);
//                        edgeList.add(edge);
//                    }
//                    return edgeList;
//                }
//            }
//        } catch (SQLException ex) {
//        	throw new DBException("Load Edge failed", ex);
//        }
//    }
//    
//    private List<? extends Neo4jEdge> loadNeo4jEdges(DBRProgressMonitor monitor) throws DBException {
//        if (edges != null) {
//            return (List<? extends Neo4jEdge>) edges;
//        }
//        
//        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load Edges")) {
//            try (JDBCPreparedStatement dbStat =
//                    session.prepareStatement("CALL db.relationshipTypes()")) {
//                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
//                    List<Neo4jEdge> edgeList = new ArrayList<Neo4jEdge>();
//                    while (dbResult.next()) {
//                        String edgeType = JDBCUtils.safeGetString(dbResult, "relationshipType");
//                        Neo4jEdge neo4jEdge = new Neo4jEdge(this.getObject(), edgeType, dbResult);
//                        edgeList.add(neo4jEdge);
//                    }
//                    return edgeList;
//                }
//            }
//        } catch (SQLException ex) {
//            throw new DBException("load Neo4j Edge", ex);
//        }
//    }
    
    
    private class CoraDbVertexCache extends JDBCObjectCache<CoradbUser, CoraDbVertex> {

		@Override
		protected JDBCStatement prepareObjectsStatement(JDBCSession session, CoradbUser owner) throws SQLException {
			final JDBCPreparedStatement dbStat;
			StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'VERTEX'");
        	sb.append(" AND owner_name = '").append(owner.getName()).append("'");
            dbStat = session.prepareStatement(sb.toString());
            return dbStat;
		}

		@Override
		protected CoraDbVertex fetchObject(JDBCSession session, CoradbUser owner, JDBCResultSet resultSet)
				throws SQLException, DBException {
                  String class_name = JDBCUtils.safeGetString(resultSet, "class_name");
                  return new CoraDbVertex(owner, class_name, "VERTEX", resultSet);
		}
    	
    }
    
    private class CoraDbEdgeCache extends JDBCObjectCache<CoradbUser, CoraDbEdge> {

		@Override
		protected JDBCStatement prepareObjectsStatement(JDBCSession session, CoradbUser owner) throws SQLException {
			final JDBCPreparedStatement dbStat;
			StringBuilder sb = new StringBuilder("select * from db_class where class_type = 'EDGE'");
        	sb.append(" AND owner_name = '").append(owner.getName()).append("'");
            dbStat = session.prepareStatement(sb.toString());
            return dbStat;
		}

		@Override
		protected CoraDbEdge fetchObject(JDBCSession session, CoradbUser owner, JDBCResultSet resultSet)
				throws SQLException, DBException {
                  String class_name = JDBCUtils.safeGetString(resultSet, "class_name");
                  return new CoraDbEdge(owner, class_name, "VERTEX", resultSet);
		}
    	
    }

}
