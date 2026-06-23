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

import java.util.List;
import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.model.DBFetchProgress;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDDataFilter;
import org.jkiss.dbeaver.model.data.DBDDataReceiver;
import org.jkiss.dbeaver.model.exec.DBCExecutionSource;
import org.jkiss.dbeaver.model.exec.DBCResultSet;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatement;
import org.jkiss.dbeaver.model.exec.DBCStatementType;
import org.jkiss.dbeaver.model.exec.DBCStatistics;
import org.jkiss.dbeaver.model.exec.DBExecUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.messages.ModelMessages;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CoraDbVertex extends GenericTable {

    private static final Log log = Log.getLog(CoraDbVertex.class);
    List<CoraDbVertexColumn> properties = null;
    
    public CoraDbVertex(
            GenericStructContainer container,
            String tableName,
            String tableType,
            JDBCResultSet dbResult) {
        super(container,tableName,tableType,dbResult);
    }
    
    @Override
    protected boolean isCacheDDL() {
        return false;
    }
    
    @Override
    public boolean isView() {
        return false;
    }
    
    @Override
    public DBCStatistics readData(
            DBCExecutionSource source,
            DBCSession session,
            DBDDataReceiver dataReceiver,
            DBDDataFilter dataFilter,
            long firstRow,
            long maxRows,
            long flags,
            int fetchSize)
            throws DBException {
        DBCStatistics statistics = new DBCStatistics();
        boolean hasLimits = firstRow >= 0 && maxRows > 0;

        DBRProgressMonitor monitor = session.getProgressMonitor();
        try {
            getAttributes(monitor);
        } catch (DBException e) {
            log.warn(e);
        }

        StringBuilder query = new StringBuilder(100);
        query.append("SELECT JSON_INFO(").append("p").append(")");
        query.append(" FROM MATCH (p:").append(getFullyQualifiedName(DBPEvaluationContext.DML)).append(")");

        String sqlQuery = query.toString();
        statistics.setQueryText(sqlQuery);

        monitor.subTask(ModelMessages.model_jdbc_fetch_table_data);

        try (DBCStatement dbStat = DBUtils.makeStatement(
            source,
            session,
            DBCStatementType.SCRIPT,
            sqlQuery,
            firstRow,
            maxRows))
        {
            if (monitor.isCanceled()) {
                return statistics;
            }
            if (dbStat instanceof JDBCStatement && (fetchSize > 0 || maxRows > 0)) {
                DBExecUtils.setStatementFetchSize(dbStat, firstRow, maxRows, fetchSize);
            }

            long startTime = System.currentTimeMillis();
            boolean executeResult = dbStat.executeStatement();
            statistics.setExecuteTime(System.currentTimeMillis() - startTime);
            if (executeResult) {
                DBCResultSet dbResult = dbStat.openResultSet();
                if (dbResult != null && !monitor.isCanceled()) {
                    try {
                        dataReceiver.fetchStart(session, dbResult, firstRow, maxRows);

                        DBFetchProgress fetchProgress = new DBFetchProgress(session.getProgressMonitor());
                        while (dbResult.nextRow()) {
                            if (fetchProgress.isCanceled() || (hasLimits && fetchProgress.isMaxRowsFetched(maxRows))) {
                                // Fetch not more than max rows
                                break;
                            }
                            dataReceiver.fetchRow(session, dbResult);
                            fetchProgress.monitorRowFetch();
                        }
                        fetchProgress.dumpStatistics(statistics);
                    } finally {
                        try {
                            dbResult.close();
                        } catch (Throwable e) {
                            log.error("Error closing result set", e); //$NON-NLS-1$
                        }
                        try {
                            dataReceiver.fetchEnd(session, dbResult);
                        } catch (Throwable e) {
                            log.error("Error while finishing result set fetch", e); //$NON-NLS-1$
                        }
                    }
                }
            }
            return statistics;
        } finally {
            dataReceiver.close();
        }
    }
    
    @Override
    public boolean supportsObjectDefinitionOption(String option) {
        return false;
    }
    
    @Property(viewable = true, order = 1)
    public String getName() {
        return super.getName();
    }
    
    @Override
    @Property(viewable = true, order = 2)
    public String getTableType() {
        return "Node";
    }
    
    @Override
    @Property(viewable = true, order = 3)
    public String getDescription() {
        return super.getDescription();
    }
    
    @Override
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options)
            throws DBException {
    	StringBuilder sb = new StringBuilder();
    	sb.append("");
        return sb.toString();
    }
    
    @Override
    public String getDDL() {
        return null;
    }
    
    @Override
    public GenericStructContainer getParentObject() {
    	return super.getParentObject();
    }
    
}
