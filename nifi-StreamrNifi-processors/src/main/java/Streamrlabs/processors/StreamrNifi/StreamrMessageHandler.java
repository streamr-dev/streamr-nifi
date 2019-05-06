package Streamrlabs.processors.StreamrNifi;


import com.streamr.client.MessageHandler;
import com.streamr.client.Subscription;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.nifi.flowfile.FlowFile;
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

    public StreamrMessageHandler() {
        this.buffer = new ArrayList();
    }

    public void setSession(ProcessSession session) {
        this.session = session;
    }

    @Override
    public void onMessage(Subscription subscription, StreamMessage streamMessage) {
        FlowFile flow = session.get();
        session.write(flow, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(streamMessage.toBytes());
            }
        });

    }
}
