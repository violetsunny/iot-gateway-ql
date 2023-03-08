package top.iot.gateway.reactor.ql.supports.fmap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.collections.CollectionUtils;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.ValueFlatMapFeature;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import top.iot.gateway.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;

/**
 * select flat_array(arr) arrValue
 */
public class ArrayValueFlatMapFeature implements ValueFlatMapFeature {

    static String ID = FeatureId.ValueFlatMap.of("flat_array").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Function function = ((Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            throw new IllegalArgumentException("函数[" + expression + "]参数不能为空");
        }

        Expression expr = function.getParameters().getExpressions().get(0);

        java.util.function.Function<ReactorQLRecord, Publisher<?>> valueMap = ValueMapFeature.createMapperNow(expr, metadata);

        return (alias, flux) -> flux
                .flatMap(record -> Flux
                        .from(valueMap.apply(record))
                        .as(CastUtils::flatStream)
                        .map(v -> record.setResult(alias, v)));
    }
}
