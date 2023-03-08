package top.iot.gateway.reactor.ql.supports.map;

import top.iot.gateway.reactor.ql.utils.CastUtils;

import java.util.function.BiFunction;

public class BinaryCalculateMapFeature extends BinaryMapFeature {

    public BinaryCalculateMapFeature(String type, BiFunction<Number, Number, Object> calculator) {
         super(type,(left,right)-> calculator.apply(CastUtils.castNumber(left), CastUtils.castNumber(right)));
    }

}
