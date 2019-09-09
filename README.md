# Fluvio

* Website: https://www.fluvio.io

Welcome to the Fluvio project!

Fluvio is a cloud-native platform for data-in-motion, built from the ground up to run on Kubernetes.  
It brings centralized control to connect, transform, and distribute real-time data across the organization.

The repository contains all the code necessary to run the Fluvio platform: Services, APIs, and the CLI.

## **Features**

- Declarative Management -  A unique approach to data management, you specify intent and fluvio does the rest.
- Cloud Native - Built for Kubernetes. 
- Real-time architecture -  Fully asynchronous by design, suitable for low latency and high throughput environments.
- Flexible Deployments - Controller can manage Cloud and on-Premise services simultaneously.
- Powerful CLI  - User-friendly and easy to use Command Line Interface.
- Written in [Rust](https://www.rust-lang.org) - [Safe](https://msrc-blog.microsoft.com/2019/07/22/why-rust-for-safe-systems-programming), Fast, Small Footprint - built for high performance distributed systems.
    - Goodbye garbage collection!
- Compatible with Kafka - Works with Kafka client and server APIs. 
    - Fluvio CLI to manage Kafka topics, as well as produce and consume Kafka logs.


## Release Status
Fluvio is at Alpha and should be suitable for lab environment. APIs, Schema, CLI, and Services are continually evolving and subject to change before R1.


## Documentation

Full, comprehensive documentation is viewable on the Fluvio website:

https://www.fluvio.io/docs



## For Developers

To learn about the Fluvio Architecture and contribute to Fluvio project, please visit the [Developer](DEVELOPER.md) section.
