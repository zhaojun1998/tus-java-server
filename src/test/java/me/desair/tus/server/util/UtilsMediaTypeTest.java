package me.desair.tus.server.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class UtilsMediaTypeTest {

  @Parameterized.Parameters(name = "{index}: {0} matches {1}: {2}")
  public static Collection<Object[]> cases() {
    String type = "application/offset+octet-stream";
    return Arrays.asList(
        new Object[][] {
          {type, type, true},
          {type + "; charset=UTF-8", type, true},
          {"Application/Offset+Octet-Stream", type, true},
          {"  " + type + "  ", type, true},
          {" Application/Offset+Octet-Stream ; charset=UTF-8; other=value ", type, true},
          {type, " Application/Offset+Octet-Stream ", true},
          {"application/octet-stream", type, false},
          {"application/json; charset=UTF-8", type, false},
          {type + "-extra; charset=UTF-8", type, false},
          {type + ", application/json", type, false},
          {null, type, false},
          {type, null, false},
          {null, null, false},
          {"", type, false},
          {"   ", type, false},
          {"; charset=UTF-8", type, false}
        });
  }

  private final String contentType;
  private final String expectedMediaType;
  private final boolean matches;

  public UtilsMediaTypeTest(String contentType, String expectedMediaType, boolean matches) {
    this.contentType = contentType;
    this.expectedMediaType = expectedMediaType;
    this.matches = matches;
  }

  @Test
  public void matchesBaseMediaType() {
    assertThat(Utils.isMediaType(contentType, expectedMediaType), is(matches));
  }
}
