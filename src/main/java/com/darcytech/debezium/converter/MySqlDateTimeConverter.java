package com.darcytech.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 处理Debezium时间转换的问题
 * Debezium默认将MySQL中datetime类型转成UTC的时间戳({@link io.debezium.time.Timestamp})，时区是写死的没法儿改，
 * 导致数据库中设置的UTC+8，到kafka中变成了多八个小时的long型时间戳
 * Debezium默认将MySQL中的timestamp类型转成UTC的字符串。
 * | mysql                               | binlog-connector                         | debezium                          |
 * | ----------------------------------- | ---------------------------------------- | --------------------------------- |
 * | date<br>(2021-01-28)                | LocalDate<br/>(2021-01-28)               | Integer<br/>(18655)               |
 * | time<br/>(17:29:04)                 | Duration<br/>(PT17H29M4S)                | Long<br/>(62944000000)            |
 * | timestamp<br/>(2021-01-28 17:29:04) | ZonedDateTime<br/>(2021-01-28T09:29:04Z) | String<br/>(2021-01-28T09:29:04Z) |
 * | Datetime<br/>(2021-01-28 17:29:04)  | LocalDateTime<br/>(2021-01-28T17:29:04)  | Long<br/>(1611854944000)          |
 *
 * @see io.debezium.connector.mysql.converters.TinyIntOneToBooleanConverter
 */
@Slf4j
public class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private Map<String, DateTimeFormatter> typeFormatterMap = buildDefaultMap();

    private static Map<String, DateTimeFormatter> buildDefaultMap() {
        HashMap<String, DateTimeFormatter> map = new HashMap<>();
        map.put("DATE", DateTimeFormatter.ISO_DATE);
        map.put("TIME", DateTimeFormatter.ISO_TIME);
        map.put("DATETIME", DateTimeFormatter.ISO_DATE_TIME);
        map.put("TIMESTAMP", DateTimeFormatter.ISO_DATE_TIME);
        return map;
    }

    @Override
    public void configure(Properties props) {
        setFormatterIfNeed(props, "format.date", "DATE");
        setFormatterIfNeed(props, "format.time", "TIME");
        setFormatterIfNeed(props, "format.datetime", "DATETIME");
        setFormatterIfNeed(props, "format.timestamp", "TIMESTAMP");
    }

    private void setFormatterIfNeed(Properties properties, String settingKey, String sqlType) {
        String formatPattern = (String) properties.get(settingKey);
        if (formatPattern == null || formatPattern.length() == 0) {
            return;
        }
        try {
            typeFormatterMap.replace(sqlType, DateTimeFormatter.ofPattern(formatPattern));
        } catch (IllegalArgumentException e) {
            log.error("The {} setting used to process the {} type is illegal:{}", settingKey, sqlType, formatPattern);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        if (!typeFormatterMap.containsKey(sqlType)) {
            return;
        }
        if (sqlType.equals("DATE")) {
            registration.register(SchemaBuilder.string().optional().name("com.darcytech.debezium.date.string"), this::convertDate);
        }
        if (sqlType.equals("TIME")) {
            registration.register(SchemaBuilder.string().optional().name("com.darcytech.debezium.time.string"), this::convertTime);
        }
        if (sqlType.equals("DATETIME")) {
            registration.register(SchemaBuilder.string().optional().name("com.darcytech.debezium.datetime.string"), this::convertDateTime);
        }
        if (sqlType.equals("TIMESTAMP")) {
            registration.register(SchemaBuilder.string().optional().name("com.darcytech.debezium.timestamp.string"), this::convertTimestamp);
        }
    }

    private String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return typeFormatterMap.get("DATE").format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return typeFormatterMap.get("DATE").format(date);
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return typeFormatterMap.get("TIME").format(time);
        }
        return null;
    }

    private String convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            return typeFormatterMap.get("DATETIME").format((LocalDateTime) input);
        }
        return null;
    }

    private String convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            return typeFormatterMap.get("TIMESTAMP").format(zonedDateTime.toLocalDateTime());
        }
        return null;
    }

}
