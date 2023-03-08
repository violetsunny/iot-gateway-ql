package top.iot.gateway.reactor.ql.supports.group;

import net.sf.jsqlparser.expression.Expression;
import top.iot.gateway.reactor.ql.ReactorQLMetadata;
import top.iot.gateway.reactor.ql.ReactorQLRecord;
import top.iot.gateway.reactor.ql.feature.FeatureId;
import top.iot.gateway.reactor.ql.feature.GroupFeature;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TraceGroupRowFeature implements GroupFeature {

    public final static String ID = FeatureId.GroupBy.of("trace").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        return flux -> flux
                .elapsed()
                .index((index, row) -> {
                    Map<String, Object> rowInfo = new HashMap<>();
                    rowInfo.put("index", index + 1); //行号
                    rowInfo.put("elapsed", row.getT1()); //自上一行数据已经过去的时间ms
                    row.getT2().addRecord("row", rowInfo);
                    return row.getT2();
                })
                .as(Flux::just);
    }


}
