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
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Test that Ionic {@link Driver} implementation works similarly to underlying driver (database INSERT).
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DriverCreateTest {

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
        final String sql = properties.getProperty("sql.insert.insecure");
        doDriver(driverClassName, sql);
        doDriverAlternate(driverClassName, sql);
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
        final String sql = properties.getProperty("sql.insert.ionic");
        doDriver(driverClassName, sql);
        doDriverAlternate(driverClassName, sql);
    }

    /**
     * Insert new record into database.
     *
     * @param driverClassName the {@link Driver} implementation to use
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors communicating with database
     */
    private void doDriver(final String driverClassName, final String sql)
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
            final PreparedStatement statement = connection.prepareStatement(sql);
            final Object[] person = PersonnelUtil.generate();
            statement.setObject(1, person[0]);
            statement.setObject(2, person[1]);
            statement.setObject(3, person[2]);
            statement.setObject(4, person[3]);
            final int rows = statement.executeUpdate();
            logger.info(String.format("ROWS = %d", rows));
            Assert.assertEquals(1, rows);
            statement.close();
        }
    }

    /**
     * Insert new record into database.  This test uses the API {@link PreparedStatement#executeUpdate(String)}, which
     * is not supported by the PostgreSQL driver.
     *
     * @param driverClassName the {@link Driver} implementation to use
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors communicating with database
     */
    private void doDriverAlternate(final String driverClassName, final String sql)
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
            final PreparedStatement statement = connection.prepareStatement(sql);
            final Object[] person = PersonnelUtil.generate();
            statement.setObject(1, person[0]);
            statement.setObject(2, person[1]);
            statement.setObject(3, person[2]);
            statement.setObject(4, person[3]);
            try {
                statement.executeUpdate(sql);
                Assert.fail("[EXCEPTION EXPECTED]");
            } catch (SQLException e) {
                // Can't use query methods that take a query string on a PreparedStatement.
                logger.info("[EXCEPTION EXPECTED] message: " + e.getMessage());
            }
        }
    }
}
