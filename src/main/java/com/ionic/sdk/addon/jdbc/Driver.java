package com.ionic.sdk.addon.jdbc;

import com.ionic.sdk.addon.jdbc.impl.IonicState;
import com.ionic.sdk.agent.Agent;
import com.ionic.sdk.error.IonicException;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implementation of JDBC interface {@link java.sql.Driver}.  Populated from IDE "Implement Methods" wizard.
 */
public class Driver implements java.sql.Driver {

    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        try {
            final Agent agent = IonicState.getAgent(info);
            final String driverClassName = info.getProperty("driverClassName");
            final Class<?> driverClass = Class.forName(driverClassName);
            final java.sql.Driver driverWrapped = (java.sql.Driver) driverClass.newInstance();
            final Connection connection = driverWrapped.connect(url, info);
            return new IonicConnection(info, connection, agent);
        } catch (IonicException e) {
            throw new SQLException(e);
        } catch (ReflectiveOperationException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(getClass().getName()).getParent();
    }
}
