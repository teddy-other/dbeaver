/*
 * The MIT License
 *
 * Copyright 2019 brunomnsilva@gmail.com.
 * Copyright (C) 2024 CUBRID Corporation.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.jkiss.dbeaver.ext.coradb.graph.graphfx.graphview.layout;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.jkiss.dbeaver.ext.coradb.graph.graphfx.graph.FxEdge;
import org.jkiss.dbeaver.ext.coradb.graph.graphfx.graph.Graph;
import org.jkiss.dbeaver.ext.coradb.graph.graphfx.graph.Vertex;
import org.jkiss.dbeaver.ext.coradb.graph.graphfx.graphview.SmartGraphPanel;
import org.jkiss.dbeaver.ext.coradb.graph.graphfx.graphview.SmartGraphVertex;

/**
 * Base class providing cycle-safe graph traversal helpers shared by all tree/group
 * placement strategies.
 */
public abstract class AbstractTreePlacementStrategy implements SmartPlacementStrategy {

    /**
     * Walks inbound edges upward to find the topmost ancestor (grand-parent) of {@code vertex}.
     * Safe against cycles: a visited-set is allocated per top-level call.
     */
    protected <V, E> SmartGraphVertex<V> searchGrandParent(
            SmartGraphPanel<V, E> smartGraphPanel,
            Graph<V, E> graph,
            SmartGraphVertex<V> vertex,
            SmartGraphVertex<V> initVertex,
            SmartGraphVertex<V> childVertex) {
        return searchGrandParentInternal(
                smartGraphPanel, graph, vertex, initVertex, childVertex, new HashSet<>());
    }

    private <V, E> SmartGraphVertex<V> searchGrandParentInternal(
            SmartGraphPanel<V, E> smartGraphPanel,
            Graph<V, E> graph,
            SmartGraphVertex<V> vertex,
            SmartGraphVertex<V> initVertex,
            SmartGraphVertex<V> childVertex,
            Set<SmartGraphVertex<V>> visited) {

        if (!visited.add(vertex)) {
            return initVertex != null ? initVertex : vertex;
        }

        Iterable<FxEdge<E, V>> inBoundEdges = graph.incomingEdges(vertex.getUnderlyingVertex());
        SmartGraphVertex<V> firstVertex = (initVertex == null) ? vertex : initVertex;

        if (((Collection<?>) inBoundEdges).size() < 1) {
            return null;
        }

        Vertex<V> parentV = null;
        SmartGraphVertex<V> smartparentV = null;
        SmartGraphVertex<V> smartGrandParentV = null;
        for (FxEdge<E, V> edge : inBoundEdges) {
            parentV = (Vertex<V>) edge.vertices()[0];
            smartparentV = smartGraphPanel.getGraphVertex(parentV);
            if (smartparentV.equals(childVertex)) {
                return firstVertex;
            }

            smartGrandParentV = searchGrandParentInternal(
                    smartGraphPanel, graph, smartparentV, firstVertex, vertex, visited);
            if (smartGrandParentV != null) {
                if (smartGrandParentV.equals(firstVertex)) {
                    return firstVertex;
                }
            }
        }

        if (smartGrandParentV == null) {
            return smartparentV;
        }

        if (smartparentV == null) {
            return vertex;
        }

        return smartGrandParentV;
    }

    /**
     * Walks outbound edges downward to find the deepest descendant (child-parent).
     * Safe against cycles: a visited-set is allocated per top-level call.
     */
    protected <V, E> SmartGraphVertex<V> searchChildParent(
            SmartGraphPanel<V, E> smartGraphPanel,
            Graph<V, E> graph,
            SmartGraphVertex<V> vertex,
            SmartGraphVertex<V> initVertex,
            SmartGraphVertex<V> childVertex) {
        return searchChildParentInternal(
                smartGraphPanel, graph, vertex, initVertex, childVertex, new HashSet<>());
    }

    private <V, E> SmartGraphVertex<V> searchChildParentInternal(
            SmartGraphPanel<V, E> smartGraphPanel,
            Graph<V, E> graph,
            SmartGraphVertex<V> vertex,
            SmartGraphVertex<V> initVertex,
            SmartGraphVertex<V> childVertex,
            Set<SmartGraphVertex<V>> visited) {

        if (!visited.add(vertex)) {
            return initVertex != null ? initVertex : vertex;
        }

        Iterable<FxEdge<E, V>> outBoundEdges = graph.outboundEdges(vertex.getUnderlyingVertex());
        SmartGraphVertex<V> firstVertex = (initVertex == null) ? vertex : initVertex;

        if (((Collection<?>) outBoundEdges).size() < 1) {
            return null;
        }

        Vertex<V> parentV = null;
        SmartGraphVertex<V> smartparentV = null;
        SmartGraphVertex<V> smartGrandParentV = null;
        for (FxEdge<E, V> edge : outBoundEdges) {
            parentV = (Vertex<V>) edge.vertices()[1];
            smartparentV = smartGraphPanel.getGraphVertex(parentV);
            if (smartparentV.equals(childVertex)) {
                return firstVertex;
            }

            smartGrandParentV = searchChildParentInternal(
                    smartGraphPanel, graph, smartparentV, firstVertex, vertex, visited);
            if (smartGrandParentV != null) {
                if (smartGrandParentV.equals(firstVertex)) {
                    return firstVertex;
                }
            }
        }

        if (smartGrandParentV == null) {
            return smartparentV;
        }

        if (smartparentV == null) {
            return vertex;
        }

        return smartGrandParentV;
    }
}
