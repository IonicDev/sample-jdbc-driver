package com.ionic.sdk.addon.jdbc.model.test;

import com.ionic.sdk.addon.jdbc.impl.IonicColumnConfig;
import com.ionic.sdk.addon.jdbc.impl.IonicConfigReader;
import com.ionic.sdk.core.codec.Transcoder;
import com.ionic.sdk.core.res.Resource;
import com.ionic.sdk.device.DeviceUtils;
import com.ionic.sdk.error.IonicException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Test ability to extract Ionic JDBC configuration data from serialized config.
 */
public class DriverConfigTest {

    /**
     * Class scoped logger.
     */
    private final Logger logger = Logger.getLogger(getClass().getName());

    @Test
    public final void test_ParseConfig() throws IonicException, SQLException {
        final byte[] resourceConfigJson = DeviceUtils.read(Resource.resolve("ionic.config.jdbc.json"));
        final String jsonString = Transcoder.utf8().encode(resourceConfigJson);
        logger.finest(jsonString);
        final String sql = "INSERT INTO personnel (first, last, zip, department) VALUES (?, ?, ?, ?)";
        final Map<Integer, IonicColumnConfig> configs = IonicConfigReader.createConfigs(jsonString, sql);

        final IonicColumnConfig columnConfigFirst = configs.get(1);
        Assert.assertNotNull(columnConfigFirst);
        final List<String> classificationFirst = columnConfigFirst.getCattrs().get("classification");
        Assert.assertNotNull(classificationFirst);
        Assert.assertTrue(classificationFirst.contains("pii"));

        final IonicColumnConfig columnConfigZip = configs.get(3);
        Assert.assertNotNull(columnConfigZip);
        final List<String> classificationZip = columnConfigZip.getCattrs().get("classification");
        Assert.assertNotNull(classificationZip);
        Assert.assertFalse(classificationZip.contains("pii"));

        final IonicColumnConfig columnConfigNull = configs.get(4);
        Assert.assertNull(columnConfigNull);
    }
}
