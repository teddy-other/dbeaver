/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
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
package org.jkiss.dbeaver.ext.cubrid.ui.views;

import org.eclipse.jface.dialogs.IDialogPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.generic.views.GenericConnectionPage;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.connection.DBPDriverLibrary;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.dialogs.connection.DriverPropertiesDialogPage;
import org.jkiss.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;

public class CubridConnectionPage extends GenericConnectionPage {

    private static final String ITEM_USE_DEFAULT = "<all enabled libraries>";

    private final List<DBPDriverLibrary> driverLibraries = new ArrayList<>();
    private Combo driverLibraryCombo;

    @Nullable
    @Override
    public IDialogPage[] getDialogPages(boolean extrasOnly, boolean forceCreate) {
        return new IDialogPage[] {
            new CubridConnectionExtraPage(),
            new DriverPropertiesDialogPage(this)
        };
    }

    @Override
    protected void createDriverLibrarySelector(Composite parent) {
        Composite libGroup = UIUtils.createComposite(parent, 2);
        GridData gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = ((GridLayout) parent.getLayout()).numColumns;
        libGroup.setLayoutData(gd);

        Label label = UIUtils.createControlLabel(libGroup, "Driver library (this connection)");
        label.setLayoutData(new GridData(GridData.HORIZONTAL_ALIGN_BEGINNING));

        driverLibraryCombo = new Combo(libGroup, SWT.DROP_DOWN | SWT.READ_ONLY);
        driverLibraryCombo.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        driverLibraryCombo.addSelectionListener(
            SelectionListener.widgetSelectedAdapter(e -> onDriverLibrarySelected()));

        refreshDriverLibraryCombo();
    }

    @Override
    public void loadSettings() {
        super.loadSettings();
        refreshDriverLibraryCombo();
    }

    @Override
    public void saveSettings(@NotNull DBPDataSourceContainer dataSource) {
        super.saveSettings(dataSource);

        String libraryId = getSelectedDriverLibraryId();
        DBPConnectionConfiguration connectionInfo = dataSource.getConnectionConfiguration();
        if (CommonUtils.isEmpty(libraryId)) {
            connectionInfo.removeProperty(DBPDriver.PROP_ACTIVE_DRIVER_LIBRARY_ID);
        } else {
            connectionInfo.setProperty(DBPDriver.PROP_ACTIVE_DRIVER_LIBRARY_ID, libraryId);
        }
    }

    @Override
    protected void updateDriverInfo(DBPDriver driver) {
        super.updateDriverInfo(driver);
        refreshDriverLibraryCombo();
    }

    private void refreshDriverLibraryCombo() {
        if (driverLibraryCombo == null || driverLibraryCombo.isDisposed()) {
            return;
        }

        driverLibraries.clear();
        driverLibraries.addAll(site.getDriver().getDriverLibraries());

        List<String> items = new ArrayList<>();
        items.add(ITEM_USE_DEFAULT);
        for (DBPDriverLibrary library : driverLibraries) {
            items.add(library.getDisplayName());
        }
        driverLibraryCombo.setItems(items.toArray(new String[0]));
        driverLibraryCombo.setEnabled(driverLibraries.size() > 1);

        // Only (re)sync from the persisted connection state - never overwrite a selection
        // that was already pushed to the live data source by onDriverLibrarySelected().
        String activeLibraryId = site.getActiveDataSource().getConnectionConfiguration()
            .getProperty(DBPDriver.PROP_ACTIVE_DRIVER_LIBRARY_ID);

        int selectionIndex = 0;
        if (!CommonUtils.isEmpty(activeLibraryId)) {
            for (int i = 0; i < driverLibraries.size(); i++) {
                if (activeLibraryId.equals(driverLibraries.get(i).getId())) {
                    selectionIndex = i + 1;
                    break;
                }
            }
        }
        driverLibraryCombo.select(selectionIndex);
    }

    @Nullable
    private String getSelectedDriverLibraryId() {
        if (driverLibraryCombo == null || driverLibraryCombo.isDisposed()) {
            return null;
        }
        int index = driverLibraryCombo.getSelectionIndex();
        if (index <= 0 || index - 1 >= driverLibraries.size()) {
            return null;
        }
        return driverLibraries.get(index - 1).getId();
    }

    private void onDriverLibrarySelected() {
        saveSettings(site.getActiveDataSource());
        site.updateButtons();
    }
}
