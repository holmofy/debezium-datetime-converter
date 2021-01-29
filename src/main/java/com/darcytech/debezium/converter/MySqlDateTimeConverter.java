package com.darcytech.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

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

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue);
        } catch (IllegalArgumentException | DateTimeException e) {
            log.error("The \"{}\" setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
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
            return dateFormatter.format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        return null;
    }

    private String convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) input);
        }
        return null;
    }

    private String convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            // mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
            return timestampFormatter.format(localDateTime);
        }
        return null;
    }

}
