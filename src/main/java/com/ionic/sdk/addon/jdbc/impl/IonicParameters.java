package com.ionic.sdk.addon.jdbc.impl;

import com.ionic.sdk.agent.Agent;
import com.ionic.sdk.agent.cipher.chunk.ChunkCipherV2;
import com.ionic.sdk.agent.cipher.chunk.data.ChunkCryptoEncryptAttributes;
import com.ionic.sdk.error.IonicException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Cache of data associated with {@link PreparedStatement}, and Ionic protection business logic.
 */
public class IonicParameters {

    /**
     * The JDBC parameters associated with the {@link java.sql.PreparedStatement}.
     */
    private final Object[] parameters;

    /**
     * The Ionic config associated with the {@link java.sql.PreparedStatement}.
     */
    private final Map<Integer, IonicColumnConfig> columnConfigs;

    /**
     * Ionic agent, used to protect data on database insert, and unprotect data on database fetch.
     */
    private final Agent agent;

    /**
     * @return Ionic agent, used to protect data on insert into database, and unprotect data on fetch from database
     */
    public Agent getAgent() {
        return agent;
    }

    public IonicParameters(final int count, final Map<Integer, IonicColumnConfig> columnConfigs, final Agent agent) {
        this.parameters = new Object[count];
        this.columnConfigs = columnConfigs;
        this.agent = agent;
    }

    /**
     * @return the number of parameters specified in the SQL of the associated {@link PreparedStatement}
     */
    public int getCount() {
        return parameters.length;
    }

    /**
     * Get the {@link PreparedStatement} parameter associated with the specified database index.
     *
     * @param dbIndex the ordinal of the parameter to be returned
     * @return the {@link PreparedStatement} parameter
     */
    public Object getParameter(final int dbIndex) {
        return parameters[dbIndex - 1];
    }

    /**
     * Set the {@link PreparedStatement} parameter associated with the specified database index.
     *
     * @param dbIndex the ordinal of the parameter to be set
     * @param value   the {@link PreparedStatement} parameter value to be set
     */
    public void setParameter(final int dbIndex, final Object value) {
        parameters[dbIndex - 1] = value;
    }


    public void clearParameters() {
        for (int index = 0; (index < parameters.length); ++index) {
            parameters[index] = null;
        }
    }

    /**
     * Before database commit of associated {@link PreparedStatement}, this function should be called to Ionic protect
     * the values specified in the Ionic configuration.
     *
     * @throws SQLException on Ionic cryptography errors
     */
    public void encrypt() throws SQLException {
        final ChunkCipherV2 chunkCipher = new ChunkCipherV2(agent);
        for (int index = 0; (index < parameters.length); ++index) {
            final int dbIndex = index + 1;
            final IonicColumnConfig columnConfig = columnConfigs.get(dbIndex);
            final Object parameter = getParameter(dbIndex);
            if ((columnConfig != null) && (parameter instanceof String)) {
                try {
                    final ChunkCryptoEncryptAttributes encryptAttributes =
                            new ChunkCryptoEncryptAttributes(columnConfig.getCattrs());
                    setParameter(dbIndex, chunkCipher.encrypt((String) parameter, encryptAttributes));
                } catch (IonicException e) {
                    throw new SQLException(e);
                }
            }
        }
    }
}
