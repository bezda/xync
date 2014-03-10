Zync
====

A fault-tolerant cluster manager for Vert.x

Zync is a fault-tolerant cluster manager for Vert.x. Zync augments the
existing Vert.x platform and cluster managers to provide support for
remote module management. This means users can deploy modules to a cluster
using the Vert.x event bus, and Zync will automatically integrate deployments
with Vert.x' failover mechanism to ensure modules remain running. Zync
also provides distributed shared data via the Vert.x event bus.

Vert.x with Zync must be run in cluster mode because Zync is designed
specifically for clustering.

### Installation
Currently, Vert.x requires some setup to use the Zync cluster manager.

1. Edit your `VERTX_HOME/conf/META-INF/services` file to read
   `net.kuujo.zync.platform.impl.ZyncPlatformManagerFactory`
1. Copy the Zync jar into the `VERTX_HOME/lib` directory.

### Starting a Zync node
Zync nodes are run as bare Vert.x instances in `-ha` mode. With the
previous setup complete, to run Zync simply start a bare Vert.x instance:

```
VERTX_HOME -ha
```

Zync follows all the standard Vert.x HA rules, including HA groups
which are integrated into the remote deployment interface as well.

## User Manual

1. [Working with deployments](#working-with-deployments)
   * [Deploying modules](#deploying-modules)
   * [Undeploying modules](#undeploying-modules)
   * [Deploying modules to specific HA groups](#deploying-modules-to-specific-ha-groups)
1. [Working with remote shared data](#working-with-remote-shared-data)
   * [Setting the value of a key](#setting-the-value-of-a-key)
   * [Reading the value of a key](#reading-the-value-of-a-key)
   * [Deleting a key](#deleting-a-key)
   * [Checking if a key exists](#checking-if-a-key-exists)
   * [Monitoring keys for changes](#monitoring-keys-for-changes)

## Working with deployments
Zync's primary purpose is to provide a simple event bus interface for
deploying modules across a cluster of Vert.x instances. This feature
was originally being developed for [Vertigo](http://github.com/kuujo/vertigo),
but I decided to abstract the feature in order to provide it as a general
use tool for Vert.x users.

### Deploying modules
Zync provides an event bus based interface for deployments. To deploy
modules via the event bus, simply send a `deploy` message to the
`zync` address.

Module deployment messages support the following options:
* `type` (optional) - The `module` deployment type is only supported
* `id` (required) - The unique deployment ID. This is required upon
   deployment since Zync deployments can span multiple instances over
   their lifetime. The deployment ID is used as a fixed reference for
   undeploying modules.
* `group` - The HA group to which to deploy the module.
* `module` - The module to deploy.
* `config` - The module configuration.
* `instances` - The number of instances to deploy.

If no HA group is specified then the default Vert.x HA group will
be used.

```java
JsonObject message = new JsonObject()
  .putString("action", "deploy")
  .putString("type", "module")
  .putString("id", "test")
  .putString("group", "some-group")
  .putString("module", "net.kuujo~some-module~1.0")
  .putObject("config", new JsonObject().putString("foo", "bar"))
  .putNumber("instances", 4);

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // Deployment was successful.
    }
  }
});
```

All Zync deployments are automatically deployed in HA mode. When multiple
instances of a deployment are specified, *all instances of the deployment
will always be started on the same node* in order to ensure the integrity
of Vert.x's class loader behaviors.

### Undeploying modules
To undeploy modules, simply send an `undeploy` message to the same
`zync` event bus address. The undeploy message must contain the deployment
ID that was assigned when the deployment was created. When failover occurs,
the assigned deployment ID carries over to the new node and is used as a
reference for undeploying the module.

```java
JsonObject message = new JsonObject()
  .putString("action", "undeploy")
  .putString("id", "test");

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // Undeployment was successful.
    }
  }
});
```

### Deploying modules to specific HA groups
In addition to the `group` option for module deployments,
modules can also be deployed directly to specific HA groups by sending
an event bus message to the group address (the name of the HA group).
Each node in each group registers a handler at the group address, and
deployment messages will be routed between different nodes by the
Vert.x event bus' standard round-robin routing.

```java
JsonObject message = new JsonObject()
  .putString("action", "deploy")
  .putString("type", "module")
  .putString("id", "test")
  .putString("module", "net.kuujo~some-module~1.0")
  .putObject("config", new JsonObject().putString("foo", "bar"))
  .putNumber("instances", 4);

vertx.eventBus().send("some-group", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // Deployment was successful.
    }
  }
});
```

## Working with remote shared data
Zync provides an event bus based API for distributed shared data.
Essentially, Zync's data support amounts to a simple key-value store,
but it also provides data-driven events, allowing users to watch
keys for changes over the Vert.x event bus.

### Setting the value of a key
To set a key in the cluster, use the `set` action, again sending
the message to the `zync` cluster.

```java
JsonObject message = new JsonObject()
  .putString("action", "set")
  .putString("key", "test")
  .putString("value", "Hello world!");

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // The key was successfully set.
    }
  }
});
```

### Reading the value of a key
To read a key from the cluster, use the `get` action. This action also
supports an additional `default` value which will be returned if the
key does not exist.

```java
JsonObject message = new JsonObject()
  .putString("action", "get")
  .putString("key", "test")
  .putString("default", "Hello world!");

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      String result = message.body().getString("result"); // Hello world!
    }
  }
});
```

### Deleting a key
To delete a key from the cluster, use the `delete` action.

```java
JsonObject message = new JsonObject()
  .putString("action", "delete")
  .putString("key", "test");

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // The delete was successfully set.
    }
  }
});
```

Note that the delete will succeed even if the key was not set.

### Checking if a key exists
To check if a key exists, use the `exists` action.

```java
JsonObject message = new JsonObject()
  .putString("action", "exists")
  .putString("key", "test");

vertx.eventBus().send("zync", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      boolean exists = message.body().getBoolean("result");
    }
  }
});
```

### Monitoring keys for changes
Zync supports monitoring keys in the clustered data store for changes.
Zync provides several events for key changes:

* `create` - when a key is created
* `change` - when a key is created or updated
* `update` - when a key is updated
* `delete` - when a key is deleted

Users can be notified of changes by registering an event bus address
with the cluster. When an event occurs for the indicated key, the
Zync cluster will send a message to the given address containing the
key, event, and value.

```java
// Register a handler at which to listen for key events.
vertx.eventBus().registerHandler("watch-test", new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    String event = message.body().getString("event");
  }
}, new Handler<AsyncResult<Void>>() {
  public void handle(AsyncResult<Void>> result) {
    if (result.succeeded()) {
      // Register the new handler with the cluster.
      JsonObject message = new JsonObject()
        .putString("action", "watch")
        .putString("key", "test")
        .putString("address", "watch-test")
        .putString("event", "change");

      vertx.eventBus().send("zync", message);
    }
  }
});
```

To stop monitoring a key for changes, simply pass the same message
but with the `unwatch` action.

```java
JsonObject message = new JsonObject()
  .putString("action", "unwatch")
  .putString("key", "test")
  .putString("address", "watch-test")
  .putString("event", "change");

vertx.eventBus().send("zync", message);
```

Note that if no `event` is indicated in the watch message, the address
will be subscribed to *all* events for the given key.
