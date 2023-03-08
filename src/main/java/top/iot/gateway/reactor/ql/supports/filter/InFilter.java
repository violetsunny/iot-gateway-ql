package top.iot.gateway.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.statement.select.SubSelect;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.FilterFeature;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import top.iot.gateway.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InFilter implements FilterFeature {

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        InExpression inExpression = ((InExpression) expression);

        Expression left = inExpression.getLeftExpression();

        ItemsList in = (inExpression.getRightItemsList());

        List<Function<ReactorQLRecord, Publisher<?>>> rightMappers = new ArrayList<>();

        if (in instanceof ExpressionList) {
            rightMappers.addAll(((ExpressionList) in).getExpressions().stream()
                    .map(exp -> ValueMapFeature.createMapperNow(exp, metadata))
                    .collect(Collectors.toList()));
        }
        if (in instanceof SubSelect) {
            rightMappers.add(ValueMapFeature.createMapperNow(((SubSelect) in), metadata));
        }

        Function<ReactorQLRecord, Publisher<?>> leftMapper = ValueMapFeature.createMapperNow(left, metadata);

        boolean not = inExpression.isNot();
        return (ctx, column) ->
                doPredicate(not,
                        asFlux(leftMapper.apply(ctx)),
                        asFlux(Flux.fromIterable(rightMappers).flatMap(mapper -> mapper.apply(ctx)))
                );
    }

    protected Flux<Object> asFlux(Publisher<?> publisher) {
        return Flux.from(publisher)
                .flatMap(v -> {
                    if (v instanceof Iterable) {
                        return Flux.fromIterable(((Iterable<?>) v));
                    }
                    if (v instanceof Publisher) {
                        return ((Publisher<?>) v);
                    }
                    if (v instanceof Map && ((Map<?, ?>) v).size() == 1) {
                        return Mono.just(((Map<?, ?>) v).values().iterator().next());
                    }
                    return Mono.just(v);
                });
    }

    protected Mono<Boolean> doPredicate(boolean not, Flux<Object> left, Flux<Object> values) {
        return values
                .flatMap(v -> left.map(l -> CompareUtils.equals(v, l)))
                .any(Boolean.TRUE::equals)
                .map(v -> not != v);
    }

    @Override
    public String getId() {
        return FeatureId.Filter.in.getId();
    }
}
