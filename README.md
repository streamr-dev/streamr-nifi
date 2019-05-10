# Apache Nifi - Streamr Processors

[Apache NiFi](https://nifi.apache.org/) is a tool created to automate data flows between systems. This repository contains the source code and latest .nar files for Streamr publish and subscribe processors for NiFi.
This version of the processors is done with NiFi v1.9.2.

## Getting Started
If you do not have NiFi installed you can find the guide and downloads for how to do it [here](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html#downloading-and-installing-nifi).

If you only wish to use the current version of the processors and not download or clone the entire repository, you can download the .nar file [here](./nifi-StreamrNifi-nar/target/nifi-StreamrNifi-nar-1.0-SNAPSHOT.nar). 

After you have cloned the repository or downloaded the .nar file you have two options to get started with the Streamr processors in NiFi. You can simply copy the .nar file to NiFi's `libexec/lib` directory. If you wish to develop the processors you should link the .nar file to NiFi's libexec/lib directory. 

`ln -s {STREAMRNIFI_HOME}/nifi-StreamrNifi-nar/target/nifi-StreamrNifi-nar-1.0-SNAPSHOT.nar {NIFI_HOME}/libexec/lib`

This way you do not need to update the .nar file to NiFi's directory every single time after compiling the processors.

## Using the processors

After the .nar file is added to the NiFi's /lib directory you should be able to use the processors. You can add new processors by dragging the add processor icon from the top left corner to the canvas. 

![Finding the processors](./docs/Finding_the_processors.png "Logo Title Text 1")

In the image above you can see an easy way to find the Streamr processors in the Add Processor module. Simply select Streamrlabs from the the Source drag down menu and the publish and subscribe processors should be displayed.

If you do not have a Streamr account or haven't yet set up a stream you can do these [Streamr's editor](https://www.streamr.com/canvas/editor). If you need help with setting up Streamr check out [this](https://medium.com/streamrblog/how-to-connect-data-to-streamr-in-5-minutes-1-of-3-9363afd254e6) blog post.

### Using the subsribe processor
After adding the subscribe processor to the flow you need to configure the stream you wish to subscribe to. This is done by first double clicking the processor, then going to the properties tab and lastly adding your Streamr API key and Stream ID to the properties. These can be found in Streamr's editor.

After the properties have been set up the processor should not work before you have either auto-terminated the relationships or connected them to nodes. 

![Running subscribe example](./docs/Subscribe_example)

### Using the publish processor

