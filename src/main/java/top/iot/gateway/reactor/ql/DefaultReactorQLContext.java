package top.iot.gateway.reactor.ql;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static top.iot.gateway.reactor.ql.utils.SqlUtils.getCleanStr;

public class DefaultReactorQLContext implements ReactorQLContext {

    private final Function<String, Flux<Object>> supplier;

    private final List<Object> parameter;

    private volatile Map<String, Object> namedParameter;

    private BiFunction<String, Flux<Object>, Flux<Object>> mapper = (s, flux) -> flux;

    public DefaultReactorQLContext(Function<String, ? extends Publisher<?>> supplier) {
        this.supplier = name -> Flux.from(supplier.apply(name));
        this.parameter = new ArrayList<>();
    }

    public DefaultReactorQLContext(Function<String, ? extends Publisher<?>> supplier, List<Object> parameter) {
        this.supplier = name -> Flux.from(supplier.apply(name));
        this.parameter = parameter;
    }

    private Map<String, Object> safeNamedParameters() {
        if (namedParameter == null) {
            synchronized (this) {
                if (namedParameter == null) {
                    namedParameter = new HashMap<>();
                }
            }
        }
        return namedParameter;
    }

    @Override
    public Map<String, Object> getParameters() {
        return safeNamedParameters();
    }

    @Override
    public ReactorQLContext bind(Object value) {
        parameter.add(value);
        return this;
    }

    @Override
    public ReactorQLContext bind(int index, Object value) {
        parameter.add(index, value);
        return this;
    }

    @Override
    public ReactorQLContext bind(String name, Object value) {
        if (name != null && value != null) {
            safeNamedParameters().put(name, value);
        }
        return this;
    }

    @Override
    public Flux<Object> getDataSource(String name) {
        name = getCleanStr(name);
        return mapper.apply(name, supplier.apply(name));
    }


    @Override
    public Optional<Object> getParameter(int index) {
        if (parameter.size() <= (index)) {
            return Optional.empty();
        }
        return Optional.ofNullable(parameter.get(index));
    }

    @Override
    public Optional<Object> getParameter(String name) {
        if (namedParameter == null || namedParameter.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(namedParameter.get(getCleanStr(name)));
    }

    @Override
    public ReactorQLContext transfer(BiFunction<String, Flux<Object>, Flux<Object>> dataSourceMapper) {
        DefaultReactorQLContext context = new DefaultReactorQLContext(supplier);
        context.mapper = dataSourceMapper;
        return context;
    }
}
