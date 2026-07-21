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

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ext.cubrid.ui.internal.CubridMessages;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.connection.DBPDriverLibrary;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.dialogs.connection.ConnectionPageAbstract;
import org.jkiss.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;

public class CubridConnectionExtraPage extends ConnectionPageAbstract {

    private static final String PROP_SHARD_TYPE = "shardType";
    private static final String PROP_SHARD_VALUE = "shardValue";
    private static final String PROP_IS_SHARD = "isShard";
    private static final String PROP_DRIVER_LIBRARY = "driverLibrary";
    private static final String SHARD_TYPE_ID = "SHARD ID";
    private static final String SHARD_TYPE_VAL = "SHARD VAL";
    private static final String DEFAULT_SHARD_VALUE = "0";

    private Combo shardTypeCombo;
    private Text shardVal;
    private Combo driverLibraryCombo;
    private final List<DBPDriverLibrary> driverLibraries = new ArrayList<>();

    public CubridConnectionExtraPage() {
        setTitle(CubridMessages.dialog_connection_cubrid_properties);
        setDescription(CubridMessages.dialog_connection_cubrid_properties_description);
    }

    @Override
    public void createControl(Composite parent) {
        Composite container = new Composite(parent, SWT.NONE);
        container.setLayout(new GridLayout(1, false));
        container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        DBPDataSourceContainer dataSource = site.getActiveDataSource();
        DBPConnectionConfiguration connectionInfo = dataSource.getConnectionConfiguration();
        boolean enableShardControls = dataSource.getName() == null || Boolean.parseBoolean(connectionInfo.getProperty(PROP_IS_SHARD));
        createShardGroup(container, enableShardControls);
        createDriverLibraryGroup(container);

        setControl(container);
        loadSettings();
    }

    private void createDriverLibraryGroup(Composite parent) {
        Composite driverGroup = UIUtils.createTitledComposite(
            parent,
            "Driver file",
            2,
            GridData.FILL_HORIZONTAL | GridData.VERTICAL_ALIGN_BEGINNING);

        driverLibraryCombo = UIUtils.createLabelCombo(driverGroup, "Driver library", SWT.DROP_DOWN | SWT.READ_ONLY);
        driverLibraryCombo.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
        driverLibraryCombo.setToolTipText("Driver library used only for this connection. Leave as <Default> to use the driver's default library.");

        // First item means "use the driver's default library"
        driverLibraryCombo.add("<Default>");
        driverLibraries.clear();

        DBPDriver driver = site.getDriver();
        if (driver != null) {
            for (DBPDriverLibrary lib : driver.getDriverLibraries()) {
                driverLibraries.add(lib);
                String name = lib.getDisplayName();
                if (lib.getPreferredVersion() != null) {
                    name += " [" + lib.getPreferredVersion() + "]";
                }
                driverLibraryCombo.add(name);
            }
        }
        driverLibraryCombo.select(0);
    }

    private void createShardGroup(Composite parent, boolean enableControls) {
    	Composite shardGroup = UIUtils.createTitledComposite(
            parent,
            CubridMessages.dialog_connection_cubrid_properties_shard_setting,
            2,
        GridData.FILL_HORIZONTAL | GridData.VERTICAL_ALIGN_BEGINNING);

        shardTypeCombo = UIUtils.createLabelCombo(shardGroup, "Shard Hint", SWT.DROP_DOWN | SWT.READ_ONLY);
        shardTypeCombo.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));
        shardTypeCombo.add(SHARD_TYPE_ID);
        shardTypeCombo.add(SHARD_TYPE_VAL);

        shardVal = UIUtils.createLabelText(shardGroup, "Value", "");
        shardVal.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));

        shardTypeCombo.setEnabled(enableControls);
        shardVal.setEnabled(enableControls);
    }

    @Override
    public void loadSettings() {
        DBPDataSourceContainer dataSource = site.getActiveDataSource();
        DBPConnectionConfiguration connectionInfo = dataSource.getConnectionConfiguration();

        String shardType = connectionInfo.getProperty(PROP_SHARD_TYPE);
        String shardValue = connectionInfo.getProperty(PROP_SHARD_VALUE);

        if (CommonUtils.isEmpty(shardType)) {
            shardType = SHARD_TYPE_ID;
        }
        if (CommonUtils.isEmpty(shardValue)) {
            shardValue = DEFAULT_SHARD_VALUE;
        }

        shardTypeCombo.setText(shardType);
        shardVal.setText(shardValue);

        if (driverLibraryCombo != null) {
            String driverLibrary = connectionInfo.getProperty(PROP_DRIVER_LIBRARY);
            int selectionIndex = 0;
            if (!CommonUtils.isEmpty(driverLibrary)) {
                for (int i = 0; i < driverLibraries.size(); i++) {
                    if (driverLibrary.equals(driverLibraries.get(i).getPath())) {
                        selectionIndex = i + 1; // +1 for the leading <Default> item
                        break;
                    }
                }
            }
            driverLibraryCombo.select(selectionIndex);
        }

        super.loadSettings();
    }

    @Override
    public void saveSettings(@NotNull DBPDataSourceContainer dataSource) {
        DBPConnectionConfiguration connectionInfo = dataSource.getConnectionConfiguration();
        if (shardTypeCombo != null && shardVal != null) {
            String shardType = shardTypeCombo.getText().trim();
            String shardValueStr = shardVal.getText().trim();
            
            int shardValue;
            try {
            	shardValue = Integer.parseInt(shardValueStr);
            } catch (NumberFormatException e) {
            	UIUtils.showMessageBox(
                    getShell(),
                    CubridMessages.dialog_connection_cubrid_properties_invalid_input_title,
                    CubridMessages.dialog_connection_cubrid_properties_invalid_input_message,
                    SWT.ICON_ERROR
                );
                return;
            }

            if (SHARD_TYPE_ID.equals(shardType) && (shardValue < 0 || shardValue > 1)) {
            	UIUtils.showMessageBox(
                    getShell(),
                    CubridMessages.dialog_connection_cubrid_properties_invalid_shard_id_title,
                    CubridMessages.dialog_connection_cubrid_properties_invalid_shard_id_message,
                    SWT.ICON_ERROR
                );
            	return;
            }

            connectionInfo.setProperty(PROP_SHARD_TYPE, shardType);
            connectionInfo.setProperty(PROP_SHARD_VALUE, shardValueStr);
        }

        if (driverLibraryCombo != null) {
            int selectionIndex = driverLibraryCombo.getSelectionIndex();
            if (selectionIndex > 0 && selectionIndex - 1 < driverLibraries.size()) {
                connectionInfo.setProperty(PROP_DRIVER_LIBRARY, driverLibraries.get(selectionIndex - 1).getPath());
            } else {
                connectionInfo.removeProperty(PROP_DRIVER_LIBRARY);
            }
        }

        super.saveSettings(dataSource);
    }

    @Override
    public boolean isComplete() {
        return true;
    }

}
