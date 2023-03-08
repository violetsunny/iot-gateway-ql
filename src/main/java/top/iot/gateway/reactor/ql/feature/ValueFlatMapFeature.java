package top.iot.gateway.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;
import top.iot.gateway.reactor.ql.supports.ExpressionVisitorAdapter;
import top.iot.gateway.reactor.ql.supports.fmap.ArrayValueFlatMapFeature;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * 查询结果平铺支持(列转行)
 *
 * @author zhouhao
 * @see ArrayValueFlatMapFeature
 * @since 1.0
 */
public interface ValueFlatMapFeature extends Feature {

    /**
     * 创建平铺转换器,转换器用于将数据源进行平铺,通常用于列转行.
     *
     * @param expression 表达式
     * @param metadata   元数据
     * @return 转换器
     */
    BiFunction</*列名*/String, /*源*/Flux<ReactorQLRecord>,/*转换结果*/ Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata);

    static BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapperNow(Expression expr, ReactorQLMetadata metadata) {
        return createMapperByExpression(expr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的操作:" + expr));
    }

    static Optional<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> createMapperByExpression(Expression expr, ReactorQLMetadata metadata) {

        AtomicReference<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> ref = new AtomicReference<>();

        //目前仅支持Function
        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(net.sf.jsqlparser.expression.Function function) {
                metadata.getFeature(FeatureId.ValueFlatMap.of(function.getName()))
                        .ifPresent(feature -> ref.set(feature.createMapper(function, metadata)));
            }
        });

        return Optional.ofNullable(ref.get());
    }
}
