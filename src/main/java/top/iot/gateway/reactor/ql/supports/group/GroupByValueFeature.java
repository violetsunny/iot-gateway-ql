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

import java.util.function.Function;

/**
 * 按运算值分组函数
 * <pre>
 *     group by type
 *
 *     group by date_format(now(),'HH:mm')
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByValueFeature implements GroupFeature {

    @Getter
    private final String id;

    public GroupByValueFeature(String type) {
        this.id = FeatureId.GroupBy.of(type).getId();
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Function<ReactorQLRecord,Publisher<?>> mapper = ValueMapFeature.createMapperNow(expression, metadata);

        return flux -> flux
                .flatMap(ctx -> Mono.from(mapper.apply(ctx)).zipWith(Mono.just(ctx)))
                .groupBy(Tuple2::getT1, Tuple2::getT2, Integer.MAX_VALUE)
                .map(Function.identity())
                ;
    }

}
