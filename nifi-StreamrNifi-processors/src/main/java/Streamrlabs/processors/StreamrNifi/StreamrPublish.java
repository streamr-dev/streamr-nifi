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
import java.util.concurrent.LinkedBlockingQueue;

import static com.streamr.client.StreamrClient.State.Connected;
import static com.streamr.client.StreamrClient.State.Connecting;

@Tags({"Publish", "Streamr", "IOT"})
@CapabilityDescription("Publishes a message to Streamr")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="json msg to Streamr", description="Input in a one line, single entry JSON string")})
@WritesAttributes({@WritesAttribute(attribute="json msg to Streamr", description="Input is directly outputted without changes")})
@InputRequirement(Requirement.INPUT_REQUIRED)

public class StreamrPublish extends AbstractProcessor {
    private StreamrClient client;
    private Stream stream;
    private ComponentLog log;
    private volatile LinkedBlockingQueue<StreamMessage> messageQueue;


    public static final PropertyDescriptor STREAMR_API_KEY = new PropertyDescriptor
            .Builder().name("STREAMR_API_KEY")
            .displayName("Streamr api key")
            .description("Your Streamr accounts API key. Your Streamr API key can be found in Streamr's editor in your profile.")
            .required(true)
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor STREAMR_STREAM_ID = new PropertyDescriptor
            .Builder().name("STREAMR_STREAM_ID")
            .displayName("Stream id")
            .description("You can find your stream's ID in Streamr's editor. You can also create a new stream in Streamr's editor if you haven't do not have a stream up.")
            .dynamic(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final Relationship SUCCESS = new Relationship
            .Builder().name("SUCCESS")
            .description("Relationship for successfully published JSON strings")
            .build();

    public static final Relationship FAILURE = new Relationship
            .Builder().name("FAILURE")
            .description("Relationship for failed publish processor inputs")
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
        setNewStreamrClient(context); // Set client
        setStream(context); // Find and set stream
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flow = session.get();
        if (flow == null) {
            return;
        }
        // Check connection and reconnect if disconnected
        if (!client.getState().equals(Connected) && !client.getState().equals(Connecting)) {
            setNewStreamrClient(context);
            setStream(context);
        }

        final byte[] messageContent = new byte[(int) flow.getSize()];
        session.read(flow, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true); // Reads the flow files contents
            }
        });
        String json = new String(messageContent); // Transfer the bytes in to a JSON string
        final ObjectMapper mapper = new ObjectMapper();
        try {
            // If this fails the JSON string isn't valid and a transfer to the FAILURE relationship is done instead.
            LinkedHashMap<String, Object> msg = mapper.readValue(json, LinkedHashMap.class); //convert the JSON string to Streamr's Java client format
            publish(msg);
            session.transfer(flow, SUCCESS); // Transfer the flow file to the SUCCESS relationships
            session.commit(); // close the flow file "transaction"
        }
        catch (Exception e) {
            // This should be reached if the input is not a valid JSON string
            session.transfer(flow, FAILURE); // Transfer the flow file to the FAILURE relationships
            session.commit(); // close the flow file "transaction"
        }
    }

    // USed to publish valid JSON strings to Streamr
    private void publish(LinkedHashMap<String, Object> msg) {
        this.client.publish(this.stream, msg);
    }

    // Sets a new Streamr client
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
