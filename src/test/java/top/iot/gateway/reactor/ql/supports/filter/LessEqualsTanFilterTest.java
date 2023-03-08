package top.iot.gateway.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class LessEqualsTanFilterTest {

    @Test
    void test() {
        LessEqualsTanFilter filter = new LessEqualsTanFilter("<=");

        assertTrue(filter.test(1, 1));

        assertTrue(filter.test(2, 3));

        assertTrue(filter.test(2, "3"));
        assertTrue(filter.test("2", "3"));

        assertTrue(filter.test('2', '3'));

        assertTrue(filter.test(LocalDateTime.now(),System.currentTimeMillis() + 1000));


    }

}