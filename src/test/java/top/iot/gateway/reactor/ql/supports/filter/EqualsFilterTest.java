package top.iot.gateway.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class EqualsFilterTest {

    @Test
    void test() {

        EqualsFilter filter = new EqualsFilter("=", false);

        assertTrue(filter.test("1", "1"));
        assertTrue(filter.test("1", 1));
        assertTrue(filter.test(1, 1));
        assertTrue(filter.test(1L, 1D));
        assertTrue(filter.test(1L, "1"));
        assertTrue(filter.test(1F, "1.0E0"));

        long now = System.currentTimeMillis();

        assertTrue(filter.test(now, new Date(now)));
        assertTrue(filter.test(new Date(now), now));


        assertFalse(filter.test("1", "1D"));


    }

    @Test
    void testNot() {

        EqualsFilter filter = new EqualsFilter("!=", true);

//        assertFalse(filter.test("1", "1"));
//        assertFalse(filter.test("1", 1));
//        assertFalse(filter.test(1, 1));
//        assertFalse(filter.test(1L, 1D));
//        assertFalse(filter.test(1L, "1"));
        assertFalse(filter.test(1F, "1.0E0"));

        long now = System.currentTimeMillis();

        assertFalse(filter.test(now, new Date(now)));
        assertFalse(filter.test(new Date(now), now));


        assertTrue(filter.test("1", "1D"));


    }

}