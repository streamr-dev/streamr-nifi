/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package Streamrlabs.processors.StreamrNifi;

import com.streamr.client.MessageHandler;
import com.streamr.client.StreamrClient;
import com.streamr.client.Subscription;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.SigningOptions;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.rest.Stream;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

@Tags({"Subscribe, Streamr, IOT"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
public class StreamrSubscribe extends AbstractProcessor {
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
        this.messageQueue = new LinkedBlockingQueue<>();
        this.subscribed = false;
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

        subscribe();
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        if (messageQueue != null && !messageQueue.isEmpty()) {
            this.messageQueue.clear();
        }
        unsubscribe();
    }

    @OnStopped
    public void onStopped(final ProcessSession context) throws  IOException {
        if (messageQueue != null && !messageQueue.isEmpty()) {
            this.messageQueue.clear();
        }
        unsubscribe();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (!subscribed) {
            subscribe();
        }
        if (messageQueue.isEmpty()) {
            return;
        }
        transferQueue(session);
    }

    private void transferQueue(ProcessSession session) {
        while (!messageQueue.isEmpty()) {
            FlowFile flow = session.create();
            final StreamMessage streamMsg = messageQueue.poll();
            flow = session.write(flow, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {

                    ObjectOutputStream oOut = new ObjectOutputStream(out);
                    oOut.writeUTF(streamMsg.toJson());
                }
            });
            session.transfer(flow, SUCCESS);
            session.commit();
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
    private void subscribe() {
        this.sub = client.subscribe(this.stream, new MessageHandler() {
            @Override
            public void onMessage(Subscription subscription, StreamMessage streamMessage) {
                try {
                    messageQueue.put(streamMessage);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        this.subscribed = true;
    }

    private void unsubscribe() {
        if (this.sub != null) {
            this.client.unsubscribe(this.sub);
            this.sub = null;
            this.subscribed = false;
        }
    }

    public void setStream(final ProcessContext context) {
        try {
            this.stream = client.getStream(context.getProperty("STREAMR_STREAM_ID").getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
