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
package org.jkiss.dbeaver.ext.coradb.ui.views;

import org.eclipse.jface.action.IContributionManager;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IWorkbenchPart;
import org.jkiss.dbeaver.ext.coradb.model.plan.CoraDbExecutionPlan;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.sql.SQLQuery;
import org.jkiss.dbeaver.ui.editors.sql.plan.simple.SQLPlanViewProviderSimple;

public class CoraDbPlanViewProvider extends SQLPlanViewProviderSimple {

    
    @Override
    public Viewer createPlanViewer(IWorkbenchPart workbenchPart, Composite parent) {
        CoraDbPlanText treeViewer = new CoraDbPlanText(workbenchPart, parent);
        return treeViewer;
    }

    @Override
    public void visualizeQueryPlan(Viewer viewer, SQLQuery query, DBCPlan plan) {        
        query.setText(((CoraDbExecutionPlan) plan).getPlanQueryString());
        fillPlan(query, plan);
        showPlan(viewer, query, plan);
    }

    @Override
    public void contributeActions(Viewer viewer, IContributionManager contributionManager, SQLQuery lastQuery, DBCPlan lastPlan) {
     
    }

    @Override
    protected void showPlan(Viewer viewer, SQLQuery query, DBCPlan plan) {
        CoraDbPlanText treeViewer = (CoraDbPlanText) viewer;
        treeViewer.showPlan(query, plan);
    }
}