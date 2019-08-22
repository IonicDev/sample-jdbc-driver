package com.ionic.sdk.addon.jdbc.test;

import com.ionic.sdk.core.res.Resource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Test that Ionic {@link Driver} implementation works similarly to underlying driver (database SELECT).
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DriverReadTest {

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
    }

    /**
     * Verify that PostgreSQL driver can fetch result set.
     *
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors querying data from database
     */
    @Test
    public final void testJdbc_1_GetPostgreSQLDriver() throws ReflectiveOperationException, SQLException {
        doDriver(properties.getProperty("driverClassName"));
    }

    /**
     * Verify that Ionic wrapper SQL driver can fetch result set.
     *
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors querying data from database
     */
    @Test
    public final void testJdbc_2_GetIonicSQLDriver() throws ReflectiveOperationException, SQLException {
        doDriver(com.ionic.sdk.addon.jdbc.Driver.class.getName());
    }

    /**
     * Load results from database.
     *
     * @param driverClassName the {@link Driver} implementation to use
     * @throws ReflectiveOperationException on inability to instantiate SQL driver
     * @throws SQLException                 on errors querying data from database
     */
    private void doDriver(final String driverClassName) throws ReflectiveOperationException, SQLException {
        final Properties propertiesJDBC = new Properties();
        propertiesJDBC.setProperty("ionic.sep", properties.getProperty("ionic.sep"));
        propertiesJDBC.setProperty("driverClassName", properties.getProperty("driverClassName"));
        propertiesJDBC.setProperty("user", properties.getProperty("jdbc.user"));
        propertiesJDBC.setProperty("password", properties.getProperty("jdbc.password"));
        final Class<?> driverClass = Class.forName(driverClassName);
        final Driver driver = (Driver) driverClass.newInstance();
        try (Connection connection = driver.connect(properties.getProperty("jdbc.url"), propertiesJDBC)) {
            Assert.assertNotNull(connection);
            logger.info(connection.getClass().getName());
            final Statement statement = connection.createStatement();
            boolean moreResults = true;
            boolean isResultSet = statement.execute("SELECT * FROM personnel");
            while (moreResults) {
                moreResults = (isResultSet ? getResultSet(statement.getResultSet()) : getUpdateCount(statement));
                isResultSet = (moreResults && statement.getMoreResults());
            }
        }
    }

    /**
     * Iterate through {@link ResultSet} rows, dumping content to log.
     *
     * @param resultSet data over which to iterate
     * @return true, iff more results are present on the underlying connection
     * @throws SQLException
     */
    private boolean getResultSet(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            final Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; ++i) {
                // the API "metaData.getColumnName(i + 1)" can be used to get the name of the operant column
                final Object columnValue = resultSet.getObject(i + 1);
                row[i] = columnValue;
            }
            logger.info(Arrays.asList(row).toString());
        }
        return false;
    }

    /**
     * Pass through to underlying {@link Statement}.
     *
     * @param statement database interaction object, used to load result sets and update counts
     * @return true, iff more results are present on the underlying connection
     */
    @SuppressWarnings("unused")
    private boolean getUpdateCount(final Statement statement) {
        return false;
    }
}
