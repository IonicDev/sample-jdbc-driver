package com.ionic.sdk.addon.jdbc;

import com.ionic.sdk.agent.Agent;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Implementation of JDBC interface {@link java.sql.Connection}.  Populated from IDE "Implement Methods" wizard.
 */
public class IonicConnection implements java.sql.Connection {

    /**
     * Database connection parameters, and Ionic wrapper configuration.
     */
    private final Properties info;

    /**
     * The wrapped object (supplied by the underlying {@link Driver}).
     */
    private final Connection wrapped;

    /**
     * Ionic agent, used to protect data on database insert, and unprotect data on database fetch.
     */
    private final Agent agent;

    /**
     * Constructor.
     *
     * @param info    database connection parameters, and Ionic wrapper configuration
     * @param wrapped {@link Connection} supplied by the underlying {@link Driver}
     * @param agent   Ionic agent, used to protect data on database insert, and to unprotect data on database fetch
     */
    public IonicConnection(final Properties info, final Connection wrapped, final Agent agent) {
        this.info = info;
        this.wrapped = wrapped;
        this.agent = agent;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new IonicStatement(wrapped.createStatement(), agent);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(sql);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrapped.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return wrapped.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrapped.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return wrapped.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        wrapped.commit();
    }

    @Override
    public void rollback() throws SQLException {
        wrapped.rollback();
    }

    @Override
    public void close() throws SQLException {
        wrapped.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrapped.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return wrapped.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        wrapped.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return wrapped.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        wrapped.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return wrapped.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        wrapped.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return wrapped.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrapped.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrapped.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new IonicStatement(wrapped.createStatement(resultSetType, resultSetConcurrency), agent);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrapped.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrapped.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        wrapped.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return wrapped.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return wrapped.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return wrapped.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        wrapped.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrapped.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new IonicStatement(wrapped.createStatement(), agent);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(
                sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(sql, autoGeneratedKeys);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(sql, columnIndexes);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        final PreparedStatement preparedStatement = wrapped.prepareStatement(sql, columnNames);
        final String ionicConfigJson = info.getProperty("ionic.config.jdbc.json");
        return new IonicPreparedStatement(preparedStatement, sql, ionicConfigJson, agent);
    }

    @Override
    public Clob createClob() throws SQLException {
        return wrapped.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return wrapped.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return wrapped.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return wrapped.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return wrapped.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrapped.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrapped.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return wrapped.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return wrapped.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrapped.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrapped.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        wrapped.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return wrapped.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrapped.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrapped.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return wrapped.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrapped.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrapped.isWrapperFor(iface);
    }
}
