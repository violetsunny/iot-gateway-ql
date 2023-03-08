package top.iot.gateway.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.GroupFeature;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 根据运算表达式来进行分组
 * <pre>
 *     group by val/10
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByBinaryFeature implements GroupFeature {

    @Getter
    private final String id;

    private final BiFunction<Object, Object, Object> mapper;

    public GroupByBinaryFeature(String type, BiFunction<Object, Object, Object> mapper) {
        this.id = FeatureId.GroupBy.of(type).getId();
        this.mapper = mapper;
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Tuple2<Function<ReactorQLRecord, Publisher<?>>,
                Function<ReactorQLRecord, Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, Publisher<?>> rightMapper = tuple2.getT2();

        return flux -> flux
                .flatMap(ctx -> Mono.zip(
                        Mono.from(leftMapper.apply(ctx)),
                        Mono.from(rightMapper.apply(ctx)), mapper)
                                    .zipWith(Mono.just(ctx)))
                .groupBy(Tuple2::getT1, Tuple2::getT2, Integer.MAX_VALUE)
                .map(Function.identity());
    }

}
