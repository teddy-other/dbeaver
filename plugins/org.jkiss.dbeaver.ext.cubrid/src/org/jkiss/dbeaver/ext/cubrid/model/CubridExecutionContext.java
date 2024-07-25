package org.jkiss.dbeaver.ext.cubrid.model;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericExecutionContext;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCRemoteInstance;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class CubridExecutionContext extends GenericExecutionContext{

    public CubridExecutionContext(JDBCRemoteInstance instance, String purpose) {
        super(instance,purpose);
    }
    
    @NotNull
    @Override
    public boolean supportsCatalogChange() {
        return false;
    }
    
    @NotNull
    @Override
    public boolean supportsSchemaChange() {
        return false;
    }
    
    @NotNull
    @Override
    public boolean refreshDefaults(DBRProgressMonitor monitor, boolean useBootstrapSettings)
            throws DBException {
        return false;
    }
    
}
