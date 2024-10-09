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
package org.jkiss.dbeaver.ext.cubrid.internal;

import org.eclipse.core.runtime.Plugin;
import org.jkiss.dbeaver.model.impl.preferences.BundlePreferenceStore;
import org.jkiss.dbeaver.model.preferences.DBPPreferenceStore;
import org.osgi.framework.BundleContext;

/**
 * The activator class controls the plug-in life cycle
 */
public class CubridActivator extends Plugin {

    // The plug-in ID
    public static final String PLUGIN_ID = "org.jkiss.dbeaver.ext.cubrid";

    // The shared instance
    private static CubridActivator plugin;
    private BundlePreferenceStore preferenceStore;
    // The preferences
    private DBPPreferenceStore preferences;

    /**
     * The constructor
     */
    public CubridActivator() {
    }

    /*
     * (non-Javadoc)
     */
    @Override
    public void start(BundleContext context) throws Exception {
        super.start(context);
        System.out.println("CubridActivator start");
        plugin = this;
        preferences = new BundlePreferenceStore(getBundle());
    }

    /*
     * (non-Javadoc)
     */
    @Override
    public void stop(BundleContext context) throws Exception {
    	System.out.println("CubridActivator stop");
        plugin = null;
        super.stop(context);
    }

    /**
     * Returns the shared instance
     *
     * @return the shared instance
     */
    public static CubridActivator getDefault() {
        return plugin;
    }

    public DBPPreferenceStore getPreferenceStore() {
    	System.out.println("getPreferenceStore");
        if (preferenceStore == null) {
            preferenceStore = new BundlePreferenceStore(getBundle());
        }
        return preferenceStore;
    }

}
