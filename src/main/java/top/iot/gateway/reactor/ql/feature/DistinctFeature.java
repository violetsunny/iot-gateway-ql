package top.iot.gateway.reactor.ql.feature;

import net.sf.jsqlparser.statement.select.Distinct;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public interface DistinctFeature extends Feature {

    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinctMapper(Distinct distinct, ReactorQLMetadata metadata);

}
