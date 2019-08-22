package com.ionic.sdk.addon.jdbc.impl;

import com.ionic.sdk.agent.Agent;
import com.ionic.sdk.agent.AgentSdk;
import com.ionic.sdk.core.res.Resource;
import com.ionic.sdk.device.profile.persistor.DeviceProfilePersistorPlainText;
import com.ionic.sdk.device.profile.persistor.ProfilePersistor;
import com.ionic.sdk.error.IonicException;
import com.ionic.sdk.error.SdkError;

import java.io.File;
import java.net.URL;
import java.security.Security;
import java.util.Properties;

/**
 * Cache an initialized {@link Agent} loaded on the first call to {@link #getAgent(Properties)}.
 */
public class IonicState {

    /**
     * Cache an initialized {@link Agent} loaded on the first call to {@link #getAgent(Properties)}.
     */
    public static Agent getAgent(final Properties properties) throws IonicException {
        AgentSdk.initialize(Security.getProvider("SunJCE"));
        return Agent.clone(SingletonHelper.getInstance(properties.getProperty("ionic.sep")));
    }

    /**
     * Helper to guard against double init.
     * <p>
     * http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
     */
    private static class SingletonHelper {

        /**
         * The per-process singleton of this object.  The first fetch of this object should trigger its implicit
         * initialization.  Subsequent fetches will return the cached value.
         */
        private static volatile Agent instance;

        /**
         * @return the per-process singleton of this object
         */
        private static Agent getInstance(final String location) throws IonicException {
            if (instance == null) {
                synchronized (SingletonHelper.class) {
                    if (instance == null) {
                        instance = getIonicAgent(location);
                    }
                }
            }
            return instance;
        }

        /**
         * Template {@link Agent} is initialized from {@link ProfilePersistor}.
         *
         * @param location path of {@link ProfilePersistor} information
         * @return initialized {@link Agent}
         * @throws IonicException on failure to initialize {@link Agent}
         */
        private static Agent getIonicAgent(final String location) throws IonicException {
            // interpret setting as a classpath resource
            final URL url = Resource.resolve(location);
            // interpret setting as a filesystem file
            final File file = new File(location);
            // load the profile from the indicated location
            if (url != null) {
                return new Agent(new DeviceProfilePersistorPlainText(url));
            } else if (file.exists()) {
                return new Agent(new DeviceProfilePersistorPlainText(file.getPath()));
            } else {
                throw new IonicException(SdkError.ISAGENT_RESOURCE_NOT_FOUND, location);
            }
        }
    }
}
