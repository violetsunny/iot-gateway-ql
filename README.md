# 用SQL来描述ReactorAPI进行数据处理

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9e72a110fc6744bcb183a630a827dba8)](https://app.codacy.com/gh/iot-gateway/reactor-ql?utm_source=github.com&utm_medium=referral&utm_content=iot-gateway/reactor-ql&utm_campaign=Badge_Grade_Settings)
[![Maven Central](https://img.shields.io/maven-central/v/org.iot-gateway/reactor-ql.svg)](http://search.maven.org/#search%7Cga%7C1%7Creactor-ql)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/https/oss.sonatype.org/content/repositories/snapshots/org/iot-gateway/reactor-ql/maven-metadata.xml.svg)](https://oss.sonatype.org/content/repositories/snapshots/org/iot-gateway/reactor-ql)
[![Build Status](https://travis-ci.com/iot-gateway/reactor-ql.svg?branch=master)](https://travis-ci.com/iot-gateway/reactor-ql)
[![codecov](https://codecov.io/gh/iot-gateway/reactor-ql/branch/master/graph/badge.svg)](https://codecov.io/gh/iot-gateway/reactor-ql)

[Reactor](https://github.com/reactor) + [JSqlParser](https://github.com/JSQLParser/JSqlParser) = ReactorQL

## 场景

1. 规则引擎,在线编写SQL来定义数据处理规则.
2. 实时统计每分钟平均温度.
3. 统计每20条滚动数据平均值.
4. ........

## 特性

1. 支持字段映射 `select name username from user`.
2. 支持聚合函数 `count`,`sum`,`avg`,`max`,`min`.
3. 支持运算 `select val/100 percent from cpu_usage`.
4. 支持分组 `select sum(val) sum from topic group by interval('10s')`. 按时间分组.
5. 支持多列分组 `select count(1) total,productId,deviceId from messages group by productId,deviceId`.
6. 支持having `select avg(temp) avgTemp from temps group by interval('10s') having avgTemp>10 `.
7. 支持case when `select case type when 1 then '警告' when 2 then '故障' else '其他' end type from topic`.
8. 支持Join `select t1.name,t2.detail from t1,t2 where t1.id = t2.id`.

## 例子

引入依赖
```xml
<dependency>
    <groupId>top.iot.gateway</groupId>
    <artifactId>reactor-ql</artifactId>
    <version>{version}</version>
</dependency>
```

用例:

```java
  ReactorQL.builder()
        .sql("select avg(this) total from test group by interval('1s') having total > 2") //按每秒分组,并计算流中数据平均值,如果平均值大于2则下游收到数据.
        .build()
        .start(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
        .doOnNext(System.out::println)
        .as(StepVerifier::create)
        .expectNextCount(4)
        .verifyComplete();
```

更多用法请看 [单元测试](https://github.com/iot-gateway/reactor-ql/blob/master/src/test/java/org/iot-gateway/reactor/ql/ReactorQLTest.java)