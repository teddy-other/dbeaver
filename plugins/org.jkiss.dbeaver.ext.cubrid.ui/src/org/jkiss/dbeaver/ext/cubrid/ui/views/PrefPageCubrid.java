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
package org.jkiss.dbeaver.ext.cubrid.ui.views;

import org.eclipse.swt.events.VerifyEvent;
import org.eclipse.swt.events.VerifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.*;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.cubrid.ui.internal.CubridMessages;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.preferences.DBPPreferenceStore;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.preferences.PreferenceStoreDelegate;
import org.jkiss.dbeaver.ui.preferences.TargetPrefPage;
import org.jkiss.dbeaver.utils.PrefUtils;

public class PrefPageCubrid extends TargetPrefPage {

	static final Log log = Log.getLog(PrefPageCubrid.class);
	public static final String PAGE_ID = "org.jkiss.dbeaver.preferences.cubrid.general";

	private Button enableDbmsOutputCheck;
	private Text dbmsOutputBufferSize;

	public PrefPageCubrid() {
		super();
		setPreferenceStore(new PreferenceStoreDelegate(DBWorkbench.getPlatform().getPreferenceStore()));
	}

	@Override
	protected boolean hasDataSourceSpecificOptions(DBPDataSourceContainer dataSourceDescriptor) {
		DBPPreferenceStore store = dataSourceDescriptor.getPreferenceStore();
		return (store.contains(CubridConstants.PREF_DBMS_OUTPUT)
				|| store.contains(CubridConstants.PREF_DBMS_OUTPUT_BUFFER_SIZE));
	}

	@Override
	protected Control createPreferenceContent(Composite parent) {
		Composite composite = UIUtils.createPlaceholder(parent, 1);

		Group miscGroup = UIUtils.createControlGroup(composite, CubridMessages.pref_page_cubrid_group_dbms_output, 2,
				GridData.FILL_HORIZONTAL, 0);
		enableDbmsOutputCheck = UIUtils.createCheckbox(miscGroup,
				CubridMessages.pref_page_cubrid_checkbox_enable_dbms_output, "", true, 2);
		dbmsOutputBufferSize = UIUtils.createLabelText(miscGroup, CubridMessages.pref_page_cubrid_label_buffer_size,
				"");
		dbmsOutputBufferSize.addVerifyListener(new VerifyListener() {
			@Override
			public void verifyText(VerifyEvent e) {
				e.doit = e.text.matches("[0-9]*");
			}
		});

		return composite;
	}

	@Override
	protected void loadPreferences(DBPPreferenceStore store) {
		enableDbmsOutputCheck.setSelection(store.getBoolean(CubridConstants.PREF_DBMS_OUTPUT));
		dbmsOutputBufferSize.setText(String.valueOf(store.getInt(CubridConstants.PREF_DBMS_OUTPUT_BUFFER_SIZE)));
	}

	@Override
	protected void savePreferences(DBPPreferenceStore store) {
		store.setValue(CubridConstants.PREF_DBMS_OUTPUT, enableDbmsOutputCheck.getSelection());
		store.setValue(CubridConstants.PREF_DBMS_OUTPUT_BUFFER_SIZE, dbmsOutputBufferSize.getText());

		PrefUtils.savePreferenceStore(store);
	}

	@Override
	protected void clearPreferences(DBPPreferenceStore store) {
		store.setToDefault(CubridConstants.PREF_DBMS_OUTPUT);
		store.setToDefault(CubridConstants.PREF_DBMS_OUTPUT_BUFFER_SIZE);
	}

	@Override
	protected boolean supportsDataSourceSpecificOptions() {
		return true;
	}

	@Override
	protected String getPropertyPageID() {
		return PAGE_ID;
	}
}
