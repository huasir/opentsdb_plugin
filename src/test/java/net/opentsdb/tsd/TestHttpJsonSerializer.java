package net.opentsdb.tsd;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for the JSON serializer.
 * <b>Note:</b> Tests for the default error handlers are in the TestHttpQuery
 * class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public final class TestHttpJsonSerializer {
    private TSDB tsdb = null;

    @Before
    public void before() throws Exception {
        tsdb = NettyMocks.getMockedHTTPTSDB();
    }

    @Test
    public void constructorDefault() {
        assertNotNull(new HttpJsonSerializer());
    }

    @Test
    public void constructorQuery() {
        HttpQuery query = NettyMocks.getQuery(tsdb, "");
        assertNotNull(new HttpJsonSerializer(query));
    }

    @Test
    public void shutdown() {
        assertNotNull(new HttpJsonSerializer().shutdown());
    }

    @Test
    public void version() {
        assertEquals("2.0.0", new HttpJsonSerializer().version());
    }

    @Test
    public void shortName() {
        assertEquals("json", new HttpJsonSerializer().shortName());
    }

    @Test
    public void requestContentType() {
        HttpJsonSerializer serdes = new HttpJsonSerializer();
        assertEquals("application/json", serdes.requestContentType());
    }

    @Test
    public void responseContentType() {
        HttpJsonSerializer serdes = new HttpJsonSerializer();
        assertEquals("application/json; charset=UTF-8", serdes.responseContentType());
    }

    @Test
    public void parseSuggestV1() throws Exception {
        HttpQuery query = NettyMocks.postQuery(tsdb, "",
                "{\"type\":\"metrics\",\"q\":\"\"}", "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        HashMap<String, String> map = serdes.parseSuggestV1();
        assertNotNull(map);
        assertEquals("metrics", map.get("type"));
    }

    @Test(expected = BadRequestException.class)
    public void parseSuggestV1NoContent() throws Exception {
        HttpQuery query = NettyMocks.postQuery(tsdb, "",
                null, "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        serdes.parseSuggestV1();
    }

    @Test(expected = BadRequestException.class)
    public void parseSuggestV1EmptyContent() throws Exception {
        HttpQuery query = NettyMocks.postQuery(tsdb, "",
                "", "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        serdes.parseSuggestV1();
    }

    @Test(expected = BadRequestException.class)
    public void parseSuggestV1NotJSON() throws Exception {
        HttpQuery query = NettyMocks.postQuery(tsdb, "",
                "This is unparsable", "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        serdes.parseSuggestV1();
    }

    @Test
    public void formatSuggestV1() throws Exception {
        HttpQuery query = NettyMocks.getQuery(tsdb, "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        final List<String> metrics = new ArrayList<String>();
        metrics.add("sys.cpu.0.system");
        ChannelBuffer cb = serdes.formatSuggestV1(metrics);
        assertNotNull(cb);
        assertEquals("[\"sys.cpu.0.system\"]",
                cb.toString(Charset.forName("UTF-8")));
    }

    @Test
    public void formatSuggestV1JSONP() throws Exception {
        HttpQuery query = NettyMocks.getQuery(tsdb, "?jsonp=func");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        final List<String> metrics = new ArrayList<String>();
        metrics.add("sys.cpu.0.system");
        ChannelBuffer cb = serdes.formatSuggestV1(metrics);
        assertNotNull(cb);
        assertEquals("func([\"sys.cpu.0.system\"])",
                cb.toString(Charset.forName("UTF-8")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void formatSuggestV1Null() throws Exception {
        HttpQuery query = NettyMocks.getQuery(tsdb, "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        serdes.formatSuggestV1(null);
    }

    @Test
    public void formatSerializersV1() throws Exception {
        HttpQuery.initializeSerializerMaps(tsdb);
        HttpQuery query = NettyMocks.getQuery(tsdb, "");
        HttpJsonSerializer serdes = new HttpJsonSerializer(query);
        assertEquals("[{\"formatters\":",
                serdes.formatSerializersV1().toString(Charset.forName("UTF-8"))
                        .substring(0, 15));
    }
}
