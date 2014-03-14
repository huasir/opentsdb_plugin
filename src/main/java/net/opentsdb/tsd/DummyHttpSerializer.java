package net.opentsdb.tsd;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;

public class DummyHttpSerializer extends HttpSerializer {

  public DummyHttpSerializer() {
    super();
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }
  
  public DummyHttpSerializer(final HttpQuery query) {
    super(query);
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }

  @Override
  public void initialize(final TSDB tsdb) {
    // nothing to do
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return new Deferred<Object>();
  }

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public String shortName() {
    return "dummy";
  }

}
