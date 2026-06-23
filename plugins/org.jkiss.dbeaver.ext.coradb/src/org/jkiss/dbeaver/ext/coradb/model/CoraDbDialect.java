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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.model.DBPIdentifierCase;
import org.jkiss.dbeaver.model.DBPKeywordType;
import org.jkiss.dbeaver.model.sql.SQLStateType;

public class CoraDbDialect extends GenericSQLDialect {

    public static final String CORADB_DIALECT_ID = "coradb";

    private static final String[] CORADB_KEYWORDS =
            new String[] {
                "ASC",
                "AND",
                "AS",
                "CREATE",
                "COUNT",
                "CALL",
                "COLLECT",
                "DELETE",
                "DISTINCT",
                "DESC",
                "EXISTS",
                "FOREACH",
                "LIMIT",
                "LOAD CSV",
                "MATCH",
                "MERGE",
                "NOT",
                "OPTIONAL MATCH",
                "OPTIONAL",
                "OR",
                "ORDER BY",
                "RETURN",
                "REMOVE",
                "SKIP",
                "SET",
                "UNWIND",
                "UNION",
                "USE",
                "WHERE",
                "WITH"
            };

    public static final String[] CORADB_FUNCTION = {
    		"NODES",
    		"RELATIONSHIPS",
    		"PATH_COMP",
    		"LIST_COMP",
    		"FIRST",
    		"LAST",
    		"TAIL",
    		"AT",
    		"SIZE",
    		"RANGE",
    		"REDUCE",
    		"JSON_INFO",
    		"JSON_PROPERTIES",
    		"JSON_GRAPH"
    };
    
    private static final String[] DDL_KEYWORDS =
            new String[] {"CREATE", "DELETE", "REMOVE", "SET", "MERGE"};

    private static final String[] QUERY_KEYWORDS =
            new String[] {"MATCH", "OPTIONAL MATCH", "MERGE"};

    private static final String[] EXEC_KEYWORDS =
            new String[] {"CALL", "EXISTS", "COUNT", "COLLECT"};

    private static final String[] TABLE_KEYWORDS =
            new String[] {"MATCH"};

    private static final String[] COLUMN_KEYWORDS =
            new String[] {"WHERE", "RETURN", "AND", "OR", "SET", "REMOVE"};

    private static final String[][] QUOTE_STRINGS = {
            {"'", "'"},
            {"\"", "\""}
    };

    public CoraDbDialect() {
        loadKeyword();
    }

    @Override
    public String getDialectId() {
        return CORADB_DIALECT_ID;
    }

    @NotNull
    @Override
    public String getDialectName() {
        return "TurboGraph++";
    }

    @Nullable
    @Override
    public String[][] getIdentifierQuoteStrings() {
        return QUOTE_STRINGS;
    }

    @Override
    public String getQuotedString(String string) {
        return super.getQuotedString(string);
    }

    @Override
    public String[] getDDLKeywords() {
        return DDL_KEYWORDS;
    }

    @Override
    public String[] getQueryKeywords() {
        return QUERY_KEYWORDS;
    }

    @Override
    public String[] getExecuteKeywords() {
        return EXEC_KEYWORDS;
    }

    private void loadKeyword() {
        Set<String> all = new HashSet<>();
        Collections.addAll(all, CORADB_KEYWORDS);
        addFunctions(Arrays.asList(CORADB_FUNCTION));
        addTableQueryKeywords(TABLE_KEYWORDS);
        addColumnQueryKeywords(COLUMN_KEYWORDS);
        

        for (String kw : all) {
            addSQLKeyword(kw);
            setKeywordIndent(kw, 1);
        }

        for (String kw : TABLE_KEYWORDS) {
            setKeywordIndent(kw, 1);
        }

        for (String kw : COLUMN_KEYWORDS) {
            setKeywordIndent(kw, 1);
        }

        addKeywords(Arrays.asList(CORADB_FUNCTION), DBPKeywordType.FUNCTION);
    }

    @Override
    public boolean isDelimiterAfterQuery() {
        return true;
    }

    @Override
    public DBPIdentifierCase storesUnquotedCase() {
        return DBPIdentifierCase.MIXED;
    }
}
