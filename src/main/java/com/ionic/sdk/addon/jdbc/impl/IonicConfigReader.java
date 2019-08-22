package com.ionic.sdk.addon.jdbc.impl;

import com.ionic.sdk.addon.jdbc.IonicPreparedStatement;
import com.ionic.sdk.agent.key.KeyAttributesMap;
import com.ionic.sdk.error.IonicException;
import com.ionic.sdk.json.JsonSource;
import com.ionic.sdk.json.JsonU;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * When an {@link IonicPreparedStatement} is created, this reader extracts the associated Ionic query configuration
 * from the Ionic JDBC config json.
 */
public class IonicConfigReader {

    public static Map<Integer, IonicColumnConfig> createConfigs(
            final String jsonString, final String sql) throws SQLException {
        try {
            return createConfigsInternal(jsonString, sql);
        } catch (IonicException e) {
            throw new SQLException(e);
        }
    }

    public static Map<Integer, IonicColumnConfig> createConfigsInternal(
            final String jsonString, final String sql) throws IonicException {
        final Map<Integer, IonicColumnConfig> columnConfigs = new TreeMap<Integer, IonicColumnConfig>();

        final JsonObject jsonObject = JsonU.getJsonObject(jsonString);
        final JsonObject jsonPreparedStatement = JsonSource.getJsonObject(
                jsonObject, PreparedStatement.class.getSimpleName());
        if (jsonPreparedStatement != null) {
            final JsonObject jsonSql = JsonSource.getJsonObject(jsonPreparedStatement, sql);
            if (jsonSql != null) {
                final JsonObject jsonIonicColumns = JsonSource.getJsonObject(jsonSql, "IonicColumns");
                final Iterator<Map.Entry<String, JsonValue>> iterator = jsonIonicColumns.entrySet().iterator();
                while (iterator.hasNext()) {
                    final Map.Entry<String, JsonValue> ionicColumn = iterator.next();
                    final JsonObject value = (JsonObject) ionicColumn.getValue();
                    final JsonObject jsonCattrs = JsonSource.getJsonObject(value, "cattrs");
                    final KeyAttributesMap cattrs = toKeyAttributes(jsonCattrs);
                    final IonicColumnConfig columnConfig = new IonicColumnConfig(cattrs);
                    columnConfigs.put(Integer.valueOf(ionicColumn.getKey()), columnConfig);
                }
            }
        }
        return columnConfigs;
    }

    private static KeyAttributesMap toKeyAttributes(final JsonObject jsonObject) {
        final KeyAttributesMap keyAttributesMap = new KeyAttributesMap();
        for (Map.Entry<String, JsonValue> ionicColumn : jsonObject.entrySet()) {
            keyAttributesMap.put(ionicColumn.getKey(), toKeyAttributesValue(ionicColumn.getValue()));
        }
        return keyAttributesMap;
    }

    private static List<String> toKeyAttributesValue(final JsonValue value) {
        final List<String> values = new ArrayList<String>();
        final JsonArray array = (JsonArray) value;
        for (JsonValue jsonValue : array) {
            values.add(JsonSource.toString(jsonValue));
        }
        return values;
    }
}
