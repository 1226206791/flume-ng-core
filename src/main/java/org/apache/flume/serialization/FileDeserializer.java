package org.apache.flume.serialization;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.INPUT_CHARSET;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FileDeserializer
  implements EventDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(FileDeserializer.class);
  private final ResettableInputStream in;
  private final Charset outputCharset;
  private final int maxLineLength;
  private volatile boolean isOpen;
  public static final String OUT_CHARSET_KEY = "outputCharset";
  public static final String CHARSET_DFLT = "UTF-8";
  public static final String MAXLINE_KEY = "maxLineLength";
  public static final int MAXLINE_DFLT = 2048;
  public static String fileSuffix;

  FileDeserializer(Context context, ResettableInputStream in)
  {
    this.in = in;
    this.outputCharset = Charset.forName(context.getString("outputCharset", "UTF-8"));

    this.maxLineLength = context.getInteger("maxLineLength", Integer.valueOf(2048)).intValue();
    fileSuffix = context.getString("fileSuffix", "\r\nflumeFileSuffix");
    this.isOpen = true;
  }

  public Event readEvent()
    throws IOException
  {
    ensureOpen();
    String line = readLine();
    if (line == null) {
      return null;
    }
    return EventBuilder.withBody(line, this.outputCharset);
  }

  public List<Event> readEvents(int numEvents)
    throws IOException
  {
    ensureOpen();
    List events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event == null) break;
      events.add(event);
    }

    return events;
  }

  public void mark() throws IOException
  {
    ensureOpen();
    this.in.mark();
  }

  public void reset() throws IOException
  {
    ensureOpen();
    this.in.reset();
  }

  public void close() throws IOException
  {
    if (this.isOpen) {
      reset();
      this.in.close();
      this.isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!this.isOpen)
      throw new IllegalStateException("Serializer has been closed");
  }

  private String readLine()
    throws IOException
  {
    StringBuilder sb = new StringBuilder();

    int readChars = 0;
    System.out.println("In File part\n");
    int c;
    while ((c = this.in.readChar()) != -1)
    {
//      if ((c == 0) || (c == 255))
//      {
//        continue;
//      }
      readChars++;

      sb.append((char)c);
    }
    sb.append(fileSuffix);

    if (readChars > 0) {
      return sb.toString();
    }
    return null;
  }

  public static class Builder
    implements EventDeserializer.Builder
  {
    public EventDeserializer build(Context context, ResettableInputStream in)
    {
      return new FileDeserializer(context, in);
    }
  }
}