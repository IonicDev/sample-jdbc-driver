package com.ionic.sdk.addon.jdbc.impl;

import com.ionic.sdk.agent.key.KeyAttributesMap;

/**
 * Container for configuration to be applied to one JDBC value / column in the context of a database write
 * operation.
 */
public class IonicColumnConfig {

    private final KeyAttributesMap cattrs;

    public IonicColumnConfig(final KeyAttributesMap cattrs) {
        this.cattrs = cattrs;
    }

    public KeyAttributesMap getCattrs() {
        return cattrs;
    }
}
