package org.jkiss.dbeaver.ext.turbographpp.graph;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.graphics.Image;
import org.jkiss.code.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads SWT images from {@value GraphUIActivator#PLUGIN_ID} bundle resources only.
 * Use instead of {@link org.jkiss.dbeaver.ui.DBeaverIcons} for graph-plugin icons so the
 * graph bundle can be distributed and used independently.
 */
public final class GraphIcons {

    private static final Map<String, Image> imageCache = new ConcurrentHashMap<>();
    private static final Map<String, ImageDescriptor> descriptorCache = new ConcurrentHashMap<>();

    private GraphIcons() {
    }

    @NotNull
    public static Image getImage(@NotNull String iconPath) {
        return imageCache.computeIfAbsent(iconPath, GraphIcons::createImage);
    }

    @NotNull
    public static ImageDescriptor getImageDescriptor(@NotNull String iconPath) {
        return descriptorCache.computeIfAbsent(iconPath, GraphIcons::resolveDescriptor);
    }

    @NotNull
    private static Image createImage(@NotNull String iconPath) {
        return getImageDescriptor(iconPath).createImage(false);
    }

    @NotNull
    private static ImageDescriptor resolveDescriptor(@NotNull String iconPath) {
        ImageDescriptor descriptor = GraphUIActivator.getImageDescriptor(iconPath);
        return descriptor != null ? descriptor : ImageDescriptor.getMissingImageDescriptor();
    }
}
