package net.opentsdb.tsd;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.*;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class FakeHttpSerialize extends HttpSerializer {
    protected String request_content_type = "application/fake";

    /** Content type to return with data from this serializer */
    protected String response_content_type = "application/fake; charset=UTF-8";

    private static final Logger LOG =
            LoggerFactory.getLogger(FakeHttpSerialize.class);

    private static TypeReference<ArrayList<IncomingDataPoint>> TR_INCOMING =
            new TypeReference<ArrayList<IncomingDataPoint>>() {
            };

    private static TypeReference<HashMap<String, List<String>>> UID_ASSIGN =
            new TypeReference<HashMap<String, List<String>>>() {
            };

    /**
     * Type reference for common string/string maps
     */
    private static TypeReference<HashMap<String, String>> TR_HASH_MAP =
            new TypeReference<HashMap<String, String>>() {
            };
    private static TypeReference<ArrayList<TreeRule>> TR_TREE_RULES =
            new TypeReference<ArrayList<TreeRule>>() {
            };
    private static TypeReference<HashMap<String, Object>> TR_HASH_MAP_OBJ =
            new TypeReference<HashMap<String, Object>>() {
            };

    public FakeHttpSerialize() {
        super();
    }

    public FakeHttpSerialize(final HttpQuery query) {
        super(query);
    }

    @Override
    public void initialize(final TSDB tsdb) {
        // nothing to see here
    }

    public Deferred<Object> shutdown() {
        return new Deferred<Object>();
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String shortName() {
        return "fake";
    }

    @Override
    public List<IncomingDataPoint> parsePutV1() {
        if (!query.hasContent()) {
            throw new BadRequestException("Missing request content");
        }

        // convert to a string so we can handle character encoding properly
        final String content = query.getContent().trim();
        final int firstbyte = content.charAt(0);
        try {
            if (firstbyte == '{') {
                final IncomingDataPoint dp =
                        JSON.parseToObject(content, IncomingDataPoint.class);
                final ArrayList<IncomingDataPoint> dps =
                        new ArrayList<IncomingDataPoint>(1);
                dps.add(dp);
                return dps;
            } else {
                return JSON.parseToObject(content, TR_INCOMING);
            }
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    @Override
    public HashMap<String, String> parseSuggestV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            return JSON.parseToObject(query.getContent(),
                    new TypeReference<HashMap<String, String>>() {
                    });
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public HashMap<String, List<String>> parseUidAssignV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            return JSON.parseToObject(json, UID_ASSIGN);
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public TSQuery parseQueryV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            return JSON.parseToObject(json, TSQuery.class);
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public UIDMeta parseUidMetaV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            return JSON.parseToObject(json, UIDMeta.class);
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public TSMeta parseTSMetaV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            return JSON.parseToObject(json, TSMeta.class);
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public Tree parseTreeV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }
        try {
            final HashMap<String, String> properties =
                    JSON.parseToObject(json, TR_HASH_MAP);

            final Tree tree = new Tree();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                // skip nulls, empty is fine, but nulls are not welcome here
                if (entry.getValue() == null) {
                    continue;
                }

                if (entry.getKey().toLowerCase().equals("treeid")) {
                    tree.setTreeId(Integer.parseInt(entry.getValue()));
                } else if (entry.getKey().toLowerCase().equals("name")) {
                    tree.setName(entry.getValue());
                } else if (entry.getKey().toLowerCase().equals("description")) {
                    tree.setDescription(entry.getValue());
                } else if (entry.getKey().toLowerCase().equals("notes")) {
                    tree.setNotes(entry.getValue());
                } else if (entry.getKey().toLowerCase().equals("enabled")) {
                    if (entry.getValue().toLowerCase().equals("true")) {
                        tree.setEnabled(true);
                    } else {
                        tree.setEnabled(false);
                    }
                } else if (entry.getKey().toLowerCase().equals("strictMatch")) {
                    if (entry.getValue().toLowerCase().equals("true")) {
                        tree.setStrictMatch(true);
                    } else {
                        tree.setStrictMatch(false);
                    }
                }
            }
            return tree;
        } catch (NumberFormatException nfe) {
            throw new BadRequestException("Unable to parse 'tree' value");
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("Unable to parse the given JSON", iae);
        }
    }

    public TreeRule parseTreeRuleV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }

        return JSON.parseToObject(json, TreeRule.class);
    }

    public List<TreeRule> parseTreeRulesV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }

        return JSON.parseToObject(json, TR_TREE_RULES);
    }

    public Map<String, Object> parseTreeTSUIDsListV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }

        return JSON.parseToObject(json, TR_HASH_MAP_OBJ);
    }

    public Annotation parseAnnotationV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }

        return JSON.parseToObject(json, Annotation.class);
    }

    public SearchQuery parseSearchQueryV1() {
        final String json = query.getContent();
        if (json == null || json.isEmpty()) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Missing message content",
                    "Supply valid JSON formatted data in the body of your request");
        }

        return JSON.parseToObject(json, SearchQuery.class);
    }

    public ChannelBuffer formatPutV1(final Map<String, Object> results) {
        return this.serializeJSON(results);
    }

    @Override
    public ChannelBuffer formatSuggestV1(final List<String> suggestions) {
        return this.serializeJSON(suggestions);
    }

    public ChannelBuffer formatSerializersV1() {
        return serializeJSON(HttpQuery.getSerializerStatus());
    }

    public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) {
        return this.serializeJSON(aggregators);
    }

    public ChannelBuffer formatVersionV1(final Map<String, String> version) {
        return this.serializeJSON(version);
    }

    public ChannelBuffer formatDropCachesV1(final Map<String, String> response) {
        return this.serializeJSON(response);
    }

    public ChannelBuffer formatUidAssignV1(final
                                           Map<String, TreeMap<String, String>> response) {
        return this.serializeJSON(response);
    }

    public ChannelBuffer formatQueryV1(final TSQuery data_query,
                                       final List<DataPoints[]> results, final List<Annotation> globals) {

        final boolean as_arrays = this.query.hasQueryStringParam("arrays");
        final String jsonp = this.query.getQueryStringParam("jsonp");

        // todo - this should be streamed at some point since it could be HUGE
        final ChannelBuffer response = ChannelBuffers.dynamicBuffer();
        final OutputStream output = new ChannelBufferOutputStream(response);
        try {
            // don't forget jsonp
            if (jsonp != null && !jsonp.isEmpty()) {
                output.write((jsonp + "(").getBytes(query.getCharset()));
            }
            JsonGenerator json = JSON.getFactory().createGenerator(output);
            json.writeStartArray();

            for (DataPoints[] separate_dps : results) {
                for (DataPoints dps : separate_dps) {
                    json.writeStartObject();

                    json.writeStringField("metric", dps.metricName());

                    json.writeFieldName("tags");
                    json.writeStartObject();
                    if (dps.getTags() != null) {
                        for (Map.Entry<String, String> tag : dps.getTags().entrySet()) {
                            json.writeStringField(tag.getKey(), tag.getValue());
                        }
                    }
                    json.writeEndObject();

                    json.writeFieldName("aggregateTags");
                    json.writeStartArray();
                    if (dps.getAggregatedTags() != null) {
                        for (String atag : dps.getAggregatedTags()) {
                            json.writeString(atag);
                        }
                    }
                    json.writeEndArray();

                    if (data_query.getShowTSUIDs()) {
                        json.writeFieldName("tsuids");
                        json.writeStartArray();
                        final List<String> tsuids = dps.getTSUIDs();
                        Collections.sort(tsuids);
                        for (String tsuid : tsuids) {
                            json.writeString(tsuid);
                        }
                        json.writeEndArray();
                    }

                    if (!data_query.getNoAnnotations()) {
                        final List<Annotation> annotations = dps.getAnnotations();
                        if (annotations != null) {
                            Collections.sort(annotations);
                            json.writeArrayFieldStart("annotations");
                            for (Annotation note : annotations) {
                                json.writeObject(note);
                            }
                            json.writeEndArray();
                        }

                        if (globals != null && !globals.isEmpty()) {
                            Collections.sort(globals);
                            json.writeArrayFieldStart("globalAnnotations");
                            for (Annotation note : globals) {
                                json.writeObject(note);
                            }
                            json.writeEndArray();
                        }
                    }

                    // now the fun stuff, dump the data
                    json.writeFieldName("dps");

                    // default is to write a map, otherwise write arrays
                    if (as_arrays) {
                        json.writeStartArray();
                        for (final DataPoint dp : dps) {
                            if (dp.timestamp() < data_query.startTime() ||
                                    dp.timestamp() > data_query.endTime()) {
                                continue;
                            }
                            final long timestamp = data_query.getMsResolution() ?
                                    dp.timestamp() : dp.timestamp() / 1000;
                            json.writeStartArray();
                            json.writeNumber(timestamp);
                            json.writeNumber(
                                    dp.isInteger() ? dp.longValue() : dp.doubleValue());
                            json.writeEndArray();
                        }
                        json.writeEndArray();
                    } else {
                        json.writeStartObject();
                        for (final DataPoint dp : dps) {
                            if (dp.timestamp() < (data_query.startTime()) ||
                                    dp.timestamp() > (data_query.endTime())) {
                                continue;
                            }
                            final long timestamp = data_query.getMsResolution() ?
                                    dp.timestamp() : dp.timestamp() / 1000;
                            json.writeNumberField(Long.toString(timestamp),
                                    dp.isInteger() ? dp.longValue() : dp.doubleValue());
                        }
                        json.writeEndObject();
                    }

                    // close the results for this particular query
                    json.writeEndObject();
                }
            }

            // close
            json.writeEndArray();
            json.close();

            if (jsonp != null && !jsonp.isEmpty()) {
                output.write(")".getBytes());
            }
            return response;
        } catch (IOException e) {
            LOG.error("Unexpected exception", e);
            throw new RuntimeException(e);
        }
    }

    public ChannelBuffer formatUidMetaV1(final UIDMeta meta) {
        return this.serializeJSON(meta);
    }

    public ChannelBuffer formatTSMetaV1(final TSMeta meta) {
        return this.serializeJSON(meta);
    }

    public ChannelBuffer formatBranchV1(final Branch branch) {
        return this.serializeJSON(branch);
    }

    public ChannelBuffer formatTreeV1(final Tree tree) {
        return this.serializeJSON(tree);
    }

    public ChannelBuffer formatTreesV1(final List<Tree> trees) {
        return this.serializeJSON(trees);
    }

    public ChannelBuffer formatTreeRuleV1(final TreeRule rule) {
        return serializeJSON(rule);
    }

    public ChannelBuffer formatTreeCollisionNotMatchedV1(
            final Map<String, String> results, final boolean is_collisions) {
        return serializeJSON(results);
    }

    public ChannelBuffer formatTreeTestV1(final
                                          HashMap<String, HashMap<String, Object>> results) {
        return serializeJSON(results);
    }

    public ChannelBuffer formatAnnotationV1(final Annotation note) {
        return serializeJSON(note);
    }

    public ChannelBuffer formatStatsV1(final List<IncomingDataPoint> stats) {
        return serializeJSON(stats);
    }

    public ChannelBuffer formatSearchResultsV1(final SearchQuery results) {
        return serializeJSON(results);
    }

    public ChannelBuffer formatConfigV1(final Config config) {
        TreeMap<String, String> map = new TreeMap<String, String>(config.getMap());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().toUpperCase().contains("PASS")) {
                map.put(entry.getKey(), "********");
            }
        }
        return serializeJSON(map);
    }

    private ChannelBuffer serializeJSON(final Object obj) {
        if (query.hasQueryStringParam("jsonp")) {
            return ChannelBuffers.wrappedBuffer(
                    JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"),
                            obj));
        }
        return ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(obj));
    }

    @Override
    public String requestContentType() {
        return this.request_content_type;
    }

    @Override
    public String responseContentType() {
        return this.response_content_type;
    }
}