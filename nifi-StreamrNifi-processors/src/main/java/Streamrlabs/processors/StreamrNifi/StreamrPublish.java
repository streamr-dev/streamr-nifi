package Streamrlabs.processors.StreamrNifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamr.client.StreamrClient;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.SigningOptions;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.rest.Stream;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;

import java.io.InputStream;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.streamr.client.StreamrClient.State.Connected;
import static com.streamr.client.StreamrClient.State.Connecting;

@Tags({"Publish", "Streamr", "IOT"})
@CapabilityDescription("Publishes a message to Streamr")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(Requirement.INPUT_REQUIRED)

public class StreamrPublish extends AbstractProcessor {
    private StreamrClient client;
    private Stream stream;
    private ComponentLog log;
    private volatile LinkedBlockingQueue<StreamMessage> messageQueue;


    public static final PropertyDescriptor STREAMR_API_KEY = new PropertyDescriptor
            .Builder().name("STREAMR_API_KEY")
            .displayName("Streamr api key")
            .description("Profile API key for Streamr. Your Streamr API key can be found in Streamr's editor under your profile.")
            .required(true)
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor STREAMR_STREAM_ID = new PropertyDescriptor
            .Builder().name("STREAMR_STREAM_ID")
            .displayName("Stream id")
            .description("You can find your stream's ID in Streamr's editor. The stream has to be created before it can be used.")
            .dynamic(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final Relationship SUCCESS = new Relationship
            .Builder().name("SUCCESS")
            .description("Relationship")
            .build();

    public static final Relationship FAILURE = new Relationship
            .Builder().name("FAILURE")
            .description("Relationship")
            .build();


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(STREAMR_STREAM_ID);
        descriptors.add(STREAMR_API_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        this.log = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        setNewStreamrClient(context);
        setStream(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flow = session.get();
        if (flow == null) {
            return;
        }
        if (!client.getState().equals(Connected) && !client.getState().equals(Connecting)) {
            setNewStreamrClient(context);
            setStream(context);
        }
        final byte[] messageContent = new byte[(int) flow.getSize()];
        session.read(flow, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        String json = new String(messageContent);
        final ObjectMapper mapper = new ObjectMapper();
        try {
            LinkedHashMap<String, Object> msg = mapper.readValue(json, LinkedHashMap.class);
            publish(msg);
            session.transfer(flow, SUCCESS);
            session.commit();
        }
        catch (Exception e) {
            session.transfer(flow, FAILURE);
            session.commit();
        }
    }

    private void publish(LinkedHashMap<String, Object> msg) {
        this.client.publish(this.stream, msg);
    }

    private void setNewStreamrClient(final ProcessContext context) {
        try {
            this.client = new StreamrClient(new StreamrClientOptions(
                    new ApiKeyAuthenticationMethod(context.getProperty("STREAMR_API_KEY").getValue()),
                    SigningOptions.getDefault(),
                    "wss://www.streamr.com/api/v1/ws?controlLayerVersion=1&messageLayerVersion=30",
                    "https://www.streamr.com/api/v1"
            ));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void setStream(final ProcessContext context) {
        try {
            this.stream = client.getStream(context.getProperty("STREAMR_STREAM_ID").getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
