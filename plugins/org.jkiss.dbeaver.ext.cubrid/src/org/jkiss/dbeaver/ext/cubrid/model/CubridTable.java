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
package org.jkiss.dbeaver.ext.cubrid.model;

import org.apache.commons.codec.binary.StringUtils;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.model.DBFetchProgress;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDDataFilter;
import org.jkiss.dbeaver.model.data.DBDDataReceiver;
import org.jkiss.dbeaver.model.data.DBDPseudoAttribute;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionSource;
import org.jkiss.dbeaver.model.exec.DBCResultSet;
import org.jkiss.dbeaver.model.exec.DBCSavepoint;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatement;
import org.jkiss.dbeaver.model.exec.DBCStatementType;
import org.jkiss.dbeaver.model.exec.DBCStatistics;
import org.jkiss.dbeaver.model.exec.DBCTransactionManager;
import org.jkiss.dbeaver.model.exec.DBExecUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.messages.ModelMessages;
import org.jkiss.dbeaver.model.meta.IPropertyValueListProvider;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLQuery;
import org.jkiss.dbeaver.model.sql.SQLScriptElement;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.sql.parser.SQLParserContext;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class CubridTable extends GenericTable
{
	private static final Log log = Log.getLog(CubridTable.class);
	
    private final PartitionCache partitionCache = new PartitionCache();
    private CubridUser owner;
    private CubridCharset charset;
    private CubridCollation collation;
    private Integer autoIncrement;
    private boolean reuseOID = true;

    public CubridTable(
            @NotNull GenericStructContainer container,
            @Nullable String tableName,
            @Nullable String tableType,
            @Nullable JDBCResultSet dbResult) {
        super(container, tableName, tableType, dbResult);

        String collationName;
        if (tableType.equals("TABLE") && dbResult != null) {
            String type = JDBCUtils.safeGetString(dbResult, CubridConstants.IS_SYSTEM_CLASS);
            this.reuseOID = (JDBCUtils.safeGetString(dbResult, CubridConstants.REUSE_OID)).equals("YES");
            collationName = JDBCUtils.safeGetString(dbResult, CubridConstants.COLLATION);
            autoIncrement = JDBCUtils.safeGetInteger(dbResult, CubridConstants.AUTO_INCREMENT_VAL);
            if (type != null) {
                this.setSystem(type.equals("YES"));
            }
        } else {
            collationName = CubridConstants.DEFAULT_COLLATION;
        }

        String charsetName = collationName.split("_")[0];
        this.owner = (CubridUser) container;
        this.charset = getDataSource().getCharset(charsetName);
        this.collation = getDataSource().getCollation(collationName);
    }

    @NotNull
    @Override
    public CubridDataSource getDataSource() {
        return (CubridDataSource) super.getDataSource();
    }

    @NotNull
    public CubridUser getParent() {
        return (CubridUser) super.getContainer();
    }

    public boolean supportsTrigger() {
        return getParent().supportsTrigger();
    }

    @Nullable
    public Collection<? extends GenericTableIndex> getIndexes(@NotNull DBRProgressMonitor monitor)
            throws DBException {
        return getParent().getCubridIndexCache().getObjects(monitor, getContainer(), this);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public List<CubridTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor)
            throws DBException {
        return (List<CubridTableColumn>) super.getAttributes(monitor);
    }
    
    @NotNull
    public Collection<CubridPartition> getPartitions(@NotNull DBRProgressMonitor monitor) throws DBException {

        return partitionCache.getAllObjects(monitor, this);
    }

    @Nullable
    @Override
    @Property(viewable = true, editable = true, updatable = true, listProvider = OwnerListProvider.class, order = 2)
    public GenericSchema getSchema() {
        return owner;
    }

    public void setSchema(@NotNull CubridUser owner) {
        this.owner = owner;
    }

    @NotNull
    public String getUniqueName() {
        if (getDataSource().getSupportMultiSchema()) {
            return this.getSchema().getName() + "." + this.getName();
        } else {
            return this.getName();
        }
    }

    @NotNull
    @Property(viewable = true, editable = true, updatable = true, listProvider = CollationListProvider.class, order = 9)
    public CubridCollation getCollation() {
        return collation;
    }

    public void setCollation(@NotNull CubridCollation collation) {
        this.collation = collation;
    }

    @NotNull
    @Property(viewable = false, editable = true, updatable = true, listProvider = CharsetListProvider.class, order = 8) 
    public CubridCharset getCharset() {
        return charset;
    }

    public void setCharset(@NotNull CubridCharset charset) {
        this.charset = charset;
        this.collation = charset == null ? null : charset.getDefaultCollation();
    }

    @Property(viewable = true, editable = true, order = 52)
    public boolean isReuseOID() {
        return reuseOID;
    }

    public void setReuseOID(boolean reuseOID) {
        this.reuseOID = reuseOID;
    }

    @Nullable
    @Property(viewable = true, editable = true, updatable = true, order = 10)
    public Integer getAutoIncrement() {
        return autoIncrement == null ? 0 : autoIncrement;
    }

    public void setAutoIncrement(@NotNull Integer autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        if (this.isSystem()) {
            return DBUtils.getFullQualifiedName(getDataSource(), this);
        } else {
            return DBUtils.getFullQualifiedName(getDataSource(), this.getSchema(), this);
        }
    }

    @NotNull
    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        getParent().getCubridIndexCache().clearObjectCache(this);
        return super.refreshObject(monitor);
    }

    public static class OwnerListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.getDataSource().getSchemas().toArray();
        }
    }
    
    static class PartitionCache extends JDBCObjectCache<CubridTable, CubridPartition> {


        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull CubridTable table) throws SQLException {
           
            StringBuilder sql = new StringBuilder("select * from db_partition where class_name = ?");
            if(table.getDataSource().getSupportMultiSchema()) {
                sql.append(" and owner_name = ?");
            }
            final JDBCPreparedStatement dbStat = session.prepareStatement(sql.toString());
            dbStat.setString(1, table.getName());
            if(table.getDataSource().getSupportMultiSchema()) {
                dbStat.setString(2, table.getSchema().getName());
            }
            
            return dbStat;
        }
    
        @Override
        protected CubridPartition fetchObject(
            @NotNull JDBCSession session,
            @NotNull CubridTable table,
            @NotNull JDBCResultSet dbResult
        ) throws SQLException, DBException {
            String partition_class_name = JDBCUtils.safeGetString(dbResult, "partition_class_name");
            String type = JDBCUtils.safeGetString(dbResult, "partition_type");
            
            return new CubridPartition(table, partition_class_name, type, dbResult);
        }
            
    }


    public static class CharsetListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.getDataSource().getCharsets().toArray();
        }
    }

    public static class CollationListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.charset.getCollations().toArray();
        }
    }
    
    @NotNull
    @Override
    public DBCStatistics readData(
        @Nullable DBCExecutionSource source,
        @NotNull DBCSession session,
        @NotNull DBDDataReceiver dataReceiver,
        @Nullable DBDDataFilter dataFilter,
        long firstRow,
        long maxRows,
        long flags,
        int fetchSize
    ) throws DBCException {
        DBCStatistics statistics = new DBCStatistics();
        boolean hasLimits = firstRow >= 0 && maxRows > 0;

        DBPDataSource dataSource = session.getDataSource();
        DBRProgressMonitor monitor = session.getProgressMonitor();
        DBCTransactionManager txnManager = DBUtils.getTransactionManager(session.getExecutionContext());
        
        LinkedHashMap<String, List<String>> sqlList = new LinkedHashMap<>();
        HashMap<String, DBCSavepoint> savePoints = new HashMap<>();
        
        processLimitTest(sqlList);

        for (String file : sqlList.keySet()) {
            log.info(String.format("file %s" ,file));
            savePoints.clear();
            for (String sqlQuery : sqlList.get(file)) {
		        monitor.subTask(ModelMessages.model_jdbc_fetch_table_data);
		        DBCStatement dbStat = null;
		        try {
                    if (sqlQuery.contains("rollback")) {
                    	if (sqlQuery.contains("savepoint")) {
                    		log.info(String.format("ROLLBACK SAVEPOINT is not support ", sqlQuery));
//                    		if (sqlQuery.contains(":")) {
//                    			log.info(String.format("ROLLBACK SAVEPOINT Query %s ", sqlQuery));
//                    			String rollName1 = sqlQuery.substring(sqlQuery.indexOf(":") + 1);
//                    			txnManager.rollback(session, savePoints.get(rollName1));
//                    		} else {
//	                    		log.info(String.format("ROLLBACK SAVEPOINT Query %s ", sqlQuery));
//	                    		String rollName2 = sqlQuery.substring(sqlQuery.indexOf("'") + 1, sqlQuery.lastIndexOf("'"));
//	                    		txnManager.rollback(session, savePoints.get(rollName2));
//                    		}
                    	} else {
                    		txnManager.rollback(session, null);
                    	}
                    } else if (sqlQuery.contains("autocommit off")) {
                    	//log.info(String.format(sqlQuery));
                    	txnManager.setAutoCommit(monitor, false);
                    } else if (sqlQuery.contains("autocommit on")) {
                    	//log.info(String.format(sqlQuery));
                    	txnManager.setAutoCommit(monitor, true);
                    } else if (sqlQuery.startsWith("commit")) {
                    	//log.info(String.format(sqlQuery));
                    	txnManager.commit(session);
                    } else if (sqlQuery.startsWith("savepoint")) {
                    	log.info(String.format("SAVEPOINT is not support ", sqlQuery));
//                    	log.info(String.format("SAVEPOINT Query %s ", sqlQuery));
//                    	String saveName = sqlQuery.substring(sqlQuery.indexOf("'") + 1, sqlQuery.lastIndexOf("'"));
//                    	savePoints.put(saveName, txnManager.setSavepoint(monitor, saveName));
                    } else {
                    	dbStat = DBUtils.makeStatement(
            		            source,
            		            session,
            		            DBCStatementType.SCRIPT,
            		            sqlQuery,
            		            firstRow,
            		            maxRows);
                    	
                    	if (monitor.isCanceled()) {
    		                return null;
    		            }
    		            boolean executeResult = dbStat.executeStatement();
    		            
    		            if (executeResult) {
    		                DBCResultSet dbResult = dbStat.openResultSet();
    		                if (dbStat.getQueryString().toLowerCase().contains("limit")) {
    			            	log.info(String.format("success limit sqlQuery %s", dbStat.getQueryString()));
    			            } 
//    		                else {
//    			            	log.info(String.format("success sqlQuery %s", dbStat.getQueryString()));
//    			            }
    		                if (dbResult != null && !monitor.isCanceled()) {
    		                    try {
    		                    	
    		                    } finally {
    	                        	dbResult.close();
    	                        }
    		                }
    		            }
    		            else {
    		            	if (dbStat.getQueryString().contains("select")) {
    		            		log.info(String.format("sqlQuery %s", dbStat.getQueryString()));
    		            	}
    		            }
                    }
		        } catch (Exception e) {
		        	if (sqlQuery.contains("select")) {
			        	log.error(String.format("file %s" ,file));
			        	if (dbStat != null) {
			        		log.error(String.format("fail sqlQuery %s", dbStat.getQueryString()));
			        	}
			        	log.error(e);
		        	}
                } finally {
                	if (dbStat != null) {
                		dbStat.close();
                	}
                }

	        }
        }
        return null;
    }
    
    
    public static void processLimitTest(LinkedHashMap<String, List<String>> sqlList) {
        Pattern pattern = Pattern.compile("(--+|/\\*|\\*/|--)");
        StringBuilder strBuilder = new StringBuilder();
        File path = new File("d:\\cubrid-testcases\\medium");
        final List<File> sqlFiles = new ArrayList<File>();
        search(sqlFiles, path, 0);
        Scanner scanner;
        boolean endQuery = false;
        for (File f : sqlFiles) {
            List<String> queries = new ArrayList<String>();
            System.out.println("file : " + f.getPath());
            try {
                scanner = new Scanner(f);
                while (scanner.hasNextLine()) {
                    String str = scanner.nextLine();
                    boolean ret = pattern.matcher(str).find(); 
                    if (!ret) {
                        endQuery = str.endsWith(";");
                        strBuilder.append(str.trim());
                        if (!endQuery) {
                            strBuilder.append(" ");
                        }
                        
                        if (endQuery) {
                            //strBuilder.append(System.lineSeparator());
                            strBuilder.deleteCharAt(strBuilder.lastIndexOf(";"));
//                          SQLQuery query = new SQLQuery(
//                                  context.getDataSource(),
//                                  strBuilder.toString(),
//                                    0,
//                                    strBuilder.length());
//                            query.setEndsWithDelimiter(false);
                            queries.add(strBuilder.toString());
                            strBuilder = new StringBuilder();
                        }
                    }
                }
                sqlList.put(f.getPath(), queries);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //System.out.println(strBuilder.toString());
//          Display.getDefault().syncExec(new Runnable(){
//                @Override
//                public void run() {
//                  try {
//                        document.replace(0, document.getLength(), strBuilder.toString());
//                    } catch (BadLocationException e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//          try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
    
    public static boolean search(List<File> sqlFiles, File f, int depth) {
        boolean isExist = (f.getName().endsWith(".sql") && (!f.isDirectory()));
//      if (isExist) {
//          if (!f.isDirectory()) {
//              System.out.println("aaaaa : " + f.getName());
//              System.out.println("aaaaa : " + f.getPath());
//              sqlFiles.add(f);
//              return true;
//          }
//      }

        if (f.isDirectory()) {
            for(File file : f.listFiles()) {
                isExist = search(sqlFiles, file, depth+1);
            }
        }
        if (isExist) {
            if (!f.isDirectory()) {
                //System.out.println(f.getName());
                sqlFiles.add(f);
            }
        }
        return isExist;
    }
    
}
