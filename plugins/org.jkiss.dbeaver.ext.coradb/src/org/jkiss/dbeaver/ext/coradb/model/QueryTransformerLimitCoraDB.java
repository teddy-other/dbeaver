package org.jkiss.dbeaver.ext.coradb.model;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformerExt;
import org.jkiss.dbeaver.model.impl.sql.QueryTransformerLimit;
import org.jkiss.dbeaver.model.sql.SQLQuery;

public class QueryTransformerLimitCoraDB extends QueryTransformerLimit 
	implements DBCQueryTransformerExt {

    public QueryTransformerLimitCoraDB() {
        super(true);
    }

    @Override
    public boolean isApplicableTo(SQLQuery query) {
        Statement statement = query.getStatement();
        return statement != null && isLimitApplicable(statement);
    }

    public boolean isLimitApplicable(Statement statement) {
        if (statement instanceof Select select
                && select.getSelectBody() instanceof PlainSelect selectBody) {
            String where = String.valueOf(selectBody.getWhere()).toUpperCase();
            if (where.contains("ROWNUM") || where.contains("INST_NUM")) {
                return false;
            }

            String having = String.valueOf(selectBody.getHaving()).toUpperCase();
            if (having.contains("GROUPBY_NUM")) {
                return false;
            }
        }

        return true;
    }
}
