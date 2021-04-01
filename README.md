# debezium-datetime-converter
Debezium custom converter is used to deal with mysql datetime type problems

| mysql                               | binlog-connector                         | debezium                          | debezium<br />schema                 |
| ----------------------------------- | ---------------------------------------- | --------------------------------- | ----------------------------------- |
| date<br>(2021-01-28)                | LocalDate<br/>(2021-01-28)               | Integer<br/>(18655)               | io.debezium.time.Date               |
| time<br/>(17:29:04)                 | Duration<br/>(PT17H29M4S)                | Long<br/>(62944000000)            | ((Duration) data).toNanos() / 1_000 |
| timestamp<br/>(2021-01-28 17:29:04) | ZonedDateTime<br/>(2021-01-28T09:29:04Z) | String<br/>(2021-01-28T09:29:04Z) | io.debezium.time.ZonedTimestamp     |
| Datetime<br/>(2021-01-28 17:29:04)  | LocalDateTime<br/>(2021-01-28T17:29:04)  | Long<br/>(1611854944000)          | io.debezium.time.Timestamp          |


# Uage

```properties
converters=datetime
datetime.type=com.darcytech.debezium.converter.MySqlDateTimeConverter
datetime.format.date=yyyy-MM-dd
datetime.format.time=HH:mm:ss
datetime.format.datetime=yyyy-MM-dd HH:mm:ss
datetime.format.timestamp=yyyy-MM-dd HH:mm:ss
datetime.format.timestamp.zone=UTC+8
```
