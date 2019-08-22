package com.ionic.sdk.addon.jdbc.test;

import com.ionic.sdk.addon.jdbc.model.test.PersonnelUtil;
import com.ionic.sdk.core.codec.Transcoder;
import com.ionic.sdk.core.res.Resource;
import com.ionic.sdk.device.DeviceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Test that Ionic {@link Driver} implementation works similarly to underlying driver (database UPDATE).
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DriverUpdateTest {

    /**
     * Class scoped logger.
     */
    private final Logger logger = Logger.getLogger(getClass().getName());

    /**
     * Configuration for the set of contained {@link Test}.
     */
    private final Properties properties = new Properties();

    /**
     * Set up for each test case to be run.
     *
     * @throws Exception on failure to read the test configuration
     */
    @Before
    public void setUp() throws Exception {
        // load test configuration: "src/test/resources/test.properties.xml"
        final URL urlTestProperties = Resource.resolve("test.properties.xml");
        Assert.assertNotNull(urlTestProperties);
        try (InputStream is = urlTestProperties.openStream()) {
            properties.loadFromXML(is);
        }
        // stash ionic JDBC config json in {@link Connection} properties
        final byte[] resourceConfigJson = DeviceUtils.read(Resource.resolve(properties.getProperty("ionic.config")));
        properties.setProperty("ionic.config.jdbc.json", Transcoder.utf8().encode(resourceConfigJson));
    }

    /**
     * Verify that PostgreSQL driver can execute SQL update.
     *
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors writing data to database
     */
    @Test
    public final void testJdbc_1_GetPostgreSQLDriver() throws ReflectiveOperationException, SQLException {
        final String driverClassName = properties.getProperty("driverClassName");
        final String sqlCreate = properties.getProperty("sql.create.insecure");
        final String sqlUpdate = properties.getProperty("sql.update.insecure");
        doDriver(driverClassName, sqlCreate, sqlUpdate);
    }

    /**
     * Verify that Ionic wrapper SQL driver can execute SQL update.
     *
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors writing data to database
     */
    @Test
    public final void testJdbc_2_GetIonicSQLDriver() throws ReflectiveOperationException, SQLException {
        final String driverClassName = com.ionic.sdk.addon.jdbc.Driver.class.getName();
        final String sqlCreate = properties.getProperty("sql.create.ionic");
        final String sqlUpdate = properties.getProperty("sql.update.ionic");
        doDriver(driverClassName, sqlCreate, sqlUpdate);
    }

    /**
     * Insert new record into database.
     *
     * @param driverClassName the {@link Driver} implementation to use
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors communicating with database
     */
    private void doDriver(final String driverClassName, final String sqlCreate, final String sqlUpdate)
            throws ReflectiveOperationException, SQLException {
        final Properties propertiesJDBC = new Properties();
        propertiesJDBC.setProperty("ionic.sep", properties.getProperty("ionic.sep"));
        propertiesJDBC.setProperty("ionic.config.jdbc.json", properties.getProperty("ionic.config.jdbc.json"));
        propertiesJDBC.setProperty("driverClassName", properties.getProperty("driverClassName"));
        propertiesJDBC.setProperty("user", properties.getProperty("jdbc.user"));
        propertiesJDBC.setProperty("password", properties.getProperty("jdbc.password"));
        final Class<?> driverClass = Class.forName(driverClassName);
        final Driver driver = (Driver) driverClass.newInstance();
        try (Connection connection = driver.connect(properties.getProperty("jdbc.url"), propertiesJDBC)) {
            Assert.assertNotNull(connection);
            logger.info(connection.getClass().getName());
            // create record
            final PreparedStatement statementCreate = connection.prepareStatement(sqlCreate);
            final Object[] person = PersonnelUtil.generate();
            statementCreate.setObject(1, person[0]);
            statementCreate.setObject(2, person[1]);
            statementCreate.setObject(3, person[2]);
            statementCreate.setObject(4, person[3]);
            boolean isResultSetCreate = statementCreate.execute();
            Assert.assertTrue(isResultSetCreate);
            final ResultSet resultSetCreate = statementCreate.getResultSet();
            final boolean isRowCreate = resultSetCreate.next();
            Assert.assertTrue(isRowCreate);
            final Integer id = (Integer) resultSetCreate.getObject(1);
            Assert.assertTrue(id > 0);
            statementCreate.close();
            // update record
            final PreparedStatement statementUpdate = connection.prepareStatement(sqlUpdate);
            statementUpdate.setObject(1, person[1] + "2");
            statementUpdate.setObject(2, id);
            final int rows = statementUpdate.executeUpdate();
            logger.info(String.format("ROWS = %d", rows));
            Assert.assertEquals(1, rows);
            statementUpdate.close();
        }
    }
}
