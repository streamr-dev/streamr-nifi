package Streamrlabs.processors.StreamrNifi;

import com.streamr.client.MessageHandler;
import com.streamr.client.StreamrClient;
import com.streamr.client.Subscription;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.SigningOptions;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.rest.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;



import java.io.IOException;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"publish", "Streamr", "IOT"})
@CapabilityDescription("Publishes a message to Streamr")

public class StreamrPublish extends AbstractProcessor {
    private StreamrClient client;
    private Stream stream;
    private Subscription sub;
    private ComponentLog log;
    private boolean subscribed;
    private volatile LinkedBlockingQueue<StreamMessage> messageQueue;


    public static final PropertyDescriptor STREAMR_API_KEY = new PropertyDescriptor
            .Builder().name("STREAMR_API_KEY")
            .displayName("Streamr api key")
            .description("Profile API key for Streamr")
            .required(true)
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor STREAMR_STREAM_ID = new PropertyDescriptor
            .Builder().name("STREAMR_STREAM_ID")
            .displayName("Stream id")
            .description("Streams ID")
            .required(true)
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship
            .Builder().name("SUCCESS")
            .description("Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(STREAMR_STREAM_ID);
        descriptors.add(STREAMR_API_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
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
    }

    public void setNewStreamrClient(final ProcessContext context) {
        try {
            this.client = new StreamrClient(new StreamrClientOptions(
                    new ApiKeyAuthenticationMethod(context.getProperty("STREAMR_API_KEY").getValue()),
                    SigningOptions.getDefault(),
                    "wss://www.streamr.com/api/v1/ws?controlLayerVersion=1&messageLayerVersion=30",
                    "https://www.streamr.com/api/v1"
            ));
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    private void publish(LinkedHashMap<String, Object> msg) {
       this.client.publish(this.stream, msg);
    }

    public void setStream(final ProcessContext context) {
        try {
            this.stream = client.getStream(context.getProperty("STREAMR_STREAM_ID").getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
