package org.jkiss.dbeaver.ext.coradb.graph;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;

/**
 * Activator for org.jkiss.dbeaver.ext.coradb.graph.
 */
public class GraphUIActivator extends AbstractUIPlugin {

    public static final String PLUGIN_ID = "org.jkiss.dbeaver.ext.coradb.graph"; //$NON-NLS-1$

    private static GraphUIActivator plugin;

    public GraphUIActivator() {
    }

    @Override
    public void start(BundleContext context) throws Exception {
        super.start(context);
        plugin = this;
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        plugin = null;
        super.stop(context);
    }

    public static GraphUIActivator getDefault() {
        return plugin;
    }

    /**
     * @param iconPath path relative to the plugin {@code icons/} folder, e.g. {@code visualization/circle_layout.png}
     */
    public static ImageDescriptor getImageDescriptor(String iconPath) {
        String path = iconPath.startsWith("icons/") ? iconPath : "icons/" + iconPath; //$NON-NLS-1$ //$NON-NLS-2$
        return imageDescriptorFromPlugin(PLUGIN_ID, path);
    }
}
