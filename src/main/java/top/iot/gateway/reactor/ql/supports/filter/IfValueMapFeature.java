package top.iot.gateway.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.FilterFeature;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class IfValueMapFeature implements ValueMapFeature {

    private static final String ID = FeatureId.ValueMap.of("if").getId();

    @Override
   public Function<ReactorQLRecord,Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);
        List<Expression> expressions;

        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions()) || expressions.size() < 2) {
            throw new IllegalArgumentException("函数参数数量必须>=2:" + expression);
        }

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> ifPredicate = FilterFeature.createPredicateNow(expressions.get(0), metadata);

        Function<ReactorQLRecord, Publisher<?>> ifMapper = ValueMapFeature.createMapperNow(expressions.get(1), metadata);
        Function<ReactorQLRecord, Publisher<?>> elseMapper = expressions.size() == 3
                ? ValueMapFeature.createMapperNow(expressions.get(2), metadata) : record -> Mono.empty();

        return (row) -> Mono
                .from(ifPredicate.apply(row, row))
                .defaultIfEmpty(false)
                .flatMap(matched -> {
                    if (matched) {
                        return Mono.from(ifMapper.apply(row));
                    } else {
                        return Mono.from(elseMapper.apply(row));
                    }
                });
    }

    @Override
    public String getId() {
        return ID;
    }
}
