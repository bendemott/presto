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

import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.ColumnMapping.doubleMapping;
import static io.prestosql.plugin.jdbc.ColumnMapping.sliceMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;

/**
 * Methods that deal with Oracle specific functionality around ColumnMapping.
 * ColumnMapping are methods returned to Presto to convert data types for specific columns in a JDBC result set.
 * These methods convert JDBC types to Presto supported types.
 * This logic is used in OracleNumberHandling.java
 */
public class OracleColumnMappings
{
    /**
     * ColumnMapping that rounds decimals and sets PRECISION and SCALE explicitly.
     *
     * In the event the Precision of a NUMERIC or DECIMAL from Oracle exceeds the supported precision of Presto's
     * Decimal Type, we will ROUND / Truncate the Decimal Type.
     */
    public static ColumnMapping roundDecimalColumnMapping(DecimalType decimalType, RoundingMode round) {
        Objects.requireNonNull(decimalType, "decimalType is null");
        Objects.requireNonNull(round, "round is null");
        return sliceMapping(decimalType, (resultSet, columnIndex) -> {
            int scale = decimalType.getScale();
            BigDecimal dec = resultSet.getBigDecimal(columnIndex);
            // round will add zeros, or truncate by rounding to ensure the digits to the right of the decimal
            // are filled to exactly SCALE digits.
            dec = dec.setScale(scale, round);
            return Decimals.encodeUnscaledValue(dec.unscaledValue());
        }, longDecimalWriteFunction(decimalType));
    }

    /**
     * Return a Double rounded to the desired scale
     */
    public static ColumnMapping roundDoubleColumnMapping(int scale, RoundingMode round) {
        Objects.requireNonNull(round, "round is null");
        return doubleMapping(DoubleType.DOUBLE, (resultSet, columnIndex) -> {
            BigDecimal value = resultSet.getBigDecimal(columnIndex);
            value = value.setScale(scale, round); // round to ensure the decimal value will fit in a double
            return value.doubleValue();
        }, doubleWriteFunction());
    }

    /**
     * Convert decimal type of unknown precision to unbounded varchar
     */
    public static ColumnMapping decimalVarcharColumnMapping(VarcharType varcharType) {
        return sliceMapping(varcharType, (resultSet, columnIndex) -> {
            BigDecimal dec = resultSet.getBigDecimal(columnIndex);
            return utf8Slice(dec.toString());
        }, varcharWriteFunction());
    }
}