/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertThrows;

public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(OracleConfig.class)
                .setSynonymsEnabled(false)
                .setUnsupportedTypeStrategy("IGNORE")
                .setAutoReconnect(true)
                .setMaxReconnects(3)
                .setConnectionTimeout(new Duration(10, TimeUnit.SECONDS))
                .setNumberExceedsLimitsMode("ROUND")
                .setNumberTypeDefault("DECIMAL")
                .setNumberZeroScaleType("")
                .setNumberNullScaleType("")
                .setNumberRoundMode("HALF_EVEN")
                .setRatioDefaultScale(OracleConfig.UNDEFINED_SCALE)
                .setDecimalDefaultScale(OracleConfig.UNDEFINED_SCALE)
                .setDoubleDefaultScale(OracleConfig.UNDEFINED_SCALE));
    }

    @Test void testScaleSettingsConflict()
    {
        OracleConfig config = new OracleConfig();
        config.setDecimalDefaultScale(OracleConfig.UNDEFINED_SCALE);
        config.setRatioDefaultScale(OracleConfig.UNDEFINED_SCALE);

        config.setDecimalDefaultScale(8);
        assertThrows(PrestoException.class, () -> config.setRatioDefaultScale(0.3f));
    }

    @Test void testOracleRoundingModeThrows()
    {
        OracleConfig config = new OracleConfig();
        config.setNumberExceedsLimitsMode("ROUND");
        config.setNumberRoundMode("UNNECESSARY");
        assertThrows(PrestoException.class, () -> config.getNumberRoundMode());
        assertThrows(IllegalArgumentException.class, () -> config.setNumberExceedsLimitsMode("ASDF"));
        assertThrows(IllegalArgumentException.class, () -> config.setNumberRoundMode("ASDF"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.auto-reconnect", "false")
                .put("oracle.max-reconnects", "5")
                .put("oracle.connection-timeout", "11s")
                .put("unsupported-type.handling-strategy", "FAIL")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.number.exceeds-limits", "ROUND")
                .put("oracle.number.default-type", "DECIMAL")
                .put("oracle.number.round-mode", "UP")
                .put("oracle.number.type.zero-scale-type", "INTEGER")
                .put("oracle.number.type.null-scale-type", "DOUBLE")
                .put("oracle.number.default-scale.ratio", String.valueOf(OracleConfig.UNDEFINED_SCALE))
                .put("oracle.number.default-scale.decimal", "14")
                .put("oracle.number.default-scale.double", "6")
                .build();

        OracleConfig expected = new OracleConfig()
                .setAutoReconnect(false)
                .setMaxReconnects(5)
                .setConnectionTimeout(new Duration(11, TimeUnit.SECONDS))
                .setUnsupportedTypeStrategy("FAIL")
                .setSynonymsEnabled(true)
                .setNumberExceedsLimitsMode("ROUND")
                .setNumberTypeDefault("DECIMAL")
                .setNumberZeroScaleType("INTEGER")
                .setNumberNullScaleType("DOUBLE")
                .setNumberRoundMode("UP")
                .setRatioDefaultScale(OracleConfig.UNDEFINED_SCALE)
                .setDecimalDefaultScale(14)
                .setDoubleDefaultScale(6);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}