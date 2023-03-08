package top.iot.gateway.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.FilterFeature;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

public class OrFilter implements FilterFeature {

    private static final  String id = FeatureId.Filter.or.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        OrExpression and = ((OrExpression) expression);

        Expression leftExpr = and.getLeftExpression();
        Expression rightExpr = and.getRightExpression();

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(leftExpr, metadata);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(rightExpr, metadata);

        // a=1 or b=1
        return (ctx, val) -> Mono.zip(
                leftPredicate.apply(ctx, val).defaultIfEmpty(false),
                rightPredicate.apply(ctx, val).defaultIfEmpty(false),
                (leftVal, rightVal) -> leftVal || rightVal);
    }


    @Override
    public String getId() {
        return id;
    }
}
