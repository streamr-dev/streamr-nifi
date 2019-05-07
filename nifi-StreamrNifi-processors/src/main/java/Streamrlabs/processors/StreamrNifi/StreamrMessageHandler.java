package Streamrlabs.processors.StreamrNifi;


import com.streamr.client.MessageHandler;
import com.streamr.client.Subscription;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;

import javax.management.relation.Relation;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Set;

import static Streamrlabs.processors.StreamrNifi.MyProcessor.MY_RELATIONSHIP;

public class StreamrMessageHandler implements MessageHandler {
    private ProcessSession session;
    private ArrayList<StreamMessage> buffer;
    private ComponentLog log;
    public StreamrMessageHandler(ComponentLog log) {
        this.buffer = new ArrayList();
        this.log = log;
    }

    public void setSession(ProcessSession session) {
        this.session = session;
    }

    @Override
    public void onMessage(Subscription subscription, StreamMessage streamMessage) {
        this.buffer.add(streamMessage);
        this.log.debug("yes");
    }

    public ArrayList<StreamMessage> getBuffer() {
        return buffer;
    }

    public ArrayList<StreamMessage> getAndClearBuffer() {
        ArrayList<StreamMessage> toReturn = this.buffer;
        this.buffer.clear();
        return toReturn;
    }

    public StreamMessage popBuffer() {
        StreamMessage toReturn = this.buffer.get(0);
        this.buffer.remove(0);
        return toReturn;
    }
}
