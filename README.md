# Overview
This project provides glue code implemented in Hazelcast Jet that performs the following key functions in support of
the Hazelcast Industrial Telematics Solution:

* Captures raw device events and GPS positions from a remote Hazelcast IMDG data grid using the EventJournal abstraction
* Filters, maps, and cleans raw device events and GPS positions and formats them into a well-defined JSON document
* Runs business rules on device events and GPS position samples to determine if a business rules violation has occurred
  (e.g. vehicle traveling over speed limit).
 
This project is designed to be run in conjunction with the Hazelcast Industrial Telematics device telemetry capture
component available on GitHub [here](https://github.com/7erry/iot).

# Running the Project
An entirely self-contained distributable package is included in the `dist/` directory. Simply run the `run.sh` script from inside that directory to run the project.

Note that the project assumes that the device telemetry component based on Hazelcast IMDG
will be running locally on the same machine, but discovery of the device telemetry component can be controlled via the
`dist/resources/remote-imdg.xml` configuration file.