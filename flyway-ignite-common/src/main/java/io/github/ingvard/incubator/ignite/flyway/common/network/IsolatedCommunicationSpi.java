/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.ingvard.incubator.ignite.flyway.common.network;

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;

/**
 * Special communication SPI implementation single-node cluster working in "isolated" mode.
 * This node doesn't establish communication connections to other nodes but
 * immediately redirects all generated communication messages to corresponding handlers within the same node.
 */
@IgniteSpiMultipleInstancesSupport(true)
public class IsolatedCommunicationSpi extends IgniteSpiAdapter implements CommunicationSpi {
    /**
     * No-op runnable.
     */
    private static final IgniteRunnable NOOP = () -> {
        // No-op.
    };

    /** Lsnr. */
    private CommunicationListener lsnr;

    /**
     * {@inheritDoc}
     */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /**
     * {@inheritDoc}
     */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /**
     * {@inheritDoc}
     */
    @Override public void sendMessage(ClusterNode destNode, Serializable msg) throws IgniteSpiException {
        lsnr.onMessage(getSpiContext().localNode().id(), msg, NOOP);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int getSentMessagesCount() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getSentBytesCount() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public int getReceivedMessagesCount() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getReceivedBytesCount() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public int getOutboundMessagesQueueSize() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void resetMetrics() {
        // No-op.
    }

    /**
     * {@inheritDoc}
     */
    @Override public void setListener(CommunicationListener lsnr) {
        this.lsnr = lsnr;
    }
}
