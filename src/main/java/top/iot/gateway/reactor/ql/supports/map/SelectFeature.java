package top.iot.gateway.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SubSelect;
import top.iot.gateway.reactor.ql.ReactorQLContext;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.FromFeature;
import top.iot.gateway.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;


public class SelectFeature implements ValueMapFeature {

    private final static String ID = FeatureId.ValueMap.select.getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        SubSelect select = ((SubSelect) expression);

        String alias = select.getAlias() != null ? select.getAlias().getName() : null;

        Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper = FromFeature.createFromMapperByFrom(select, metadata);

        return record -> mapper
                .apply(record.getContext()
                        .transfer((table, source) -> source
                                .map(val -> ReactorQLRecord
                                        .newRecord(alias, val, record.getContext())
                                        .addRecords(record.getRecords(false))))
                        .bindAll(record.getRecords(false))
                )
                .map(ReactorQLRecord::getRecord);

    }

    @Override
    public String getId() {
        return ID;
    }
}
