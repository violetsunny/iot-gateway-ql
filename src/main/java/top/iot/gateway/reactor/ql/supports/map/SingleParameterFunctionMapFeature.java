package top.iot.gateway.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.function.Function;


public class SingleParameterFunctionMapFeature implements ValueMapFeature {

    @Getter
    private final String id;

    private final Function<Object, Object> calculator;

    public SingleParameterFunctionMapFeature(String type, Function<Object, Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.calculator = calculator;
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expressions;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions())) {
            throw new UnsupportedOperationException("函数必须指定参数:" + expression);
        }

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expressions.get(0), metadata);

        return v -> Flux.from(mapper.apply(v)).map(calculator);
    }


}
