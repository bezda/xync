Xync
====

Xync is a fault-tolerant cluster manager for Vert.x. Xync augments the
existing Vert.x platform and cluster managers to provide support for
remote module management. This means users can deploy modules to a cluster
using the Vert.x event bus, and Xync will make sure your modules continue
to run with a failover system similar to that of the core Vert.x HA mechanism.
Xync also provides distributed shared data via the Vert.x event bus.

Xync only supports Vert.x clusters run using the default Hazelcast
cluster manager. All shared data are backed by Hazelcast.

## User Manual

1. [Starting a Xync node](#starting-a-xync-node)
1. [Working with deployments](#working-with-deployments)
   * [Deploying modules](#deploying-modules)
   * [Undeploying modules](#undeploying-modules)
   * [Deploying modules to specific HA groups](#deploying-modules-to-specific-ha-groups)
1. [Working with cluster-wide shared data](#working-with-cluster-wide-shared-data)
   * [Shared maps](#shared-maps)
   * [Shared lists](#shared-lists)
   * [Shared sets](#shared-sets)
   * [Shared queues](#shared-queues)

### Starting a Xync node
Xync nodes are run as bare Vert.x instances in `-ha` mode. With the
previous setup complete, to run Xync simply start a bare Vert.x instance:

```
VERTX_HOME/bin/vertx -ha
```

Xync follows all the standard Vert.x HA rules, including HA groups
which are integrated into the remote deployment interface as well.

### Starting a Xync node
To start a Xync node just deploy the Xync module.

```java
vertx runmod net.kuujo~xync~0.1.0-SNAPSHOT
```

The module accept a few configuration options:
* `cluster` - indicates the cluster to which the node belongs. This is used as the
  event bus address at which you can send messages to the cluster. Defaults to `cluster`
* `address` - indicates the specific address of this node. By default a unique random
  address will be generated.
* `group` - indicates the group to which the node belongs. This may also be used
  to direct deployment messages at the specific group. Defaults to `__DEFAULT__`

## Working with deployments
Xync's primary purpose is to provide a simple event bus interface for
deploying modules across a cluster of Vert.x instances. This feature
was originally being developed for [Vertigo](http://github.com/kuujo/vertigo),
but I decided to abstract the feature in order to provide it as a general
use tool for Vert.x users.

### Deploying modules
Xync provides an event bus based interface for deployments. To deploy
modules via the event bus, simply send a `deploy` message to the
`cluster` address.

Module deployment messages support the following options:
* `type` (optional) - The `module` deployment type is only supported
* `id` (required) - The unique deployment ID. This is required upon
   deployment since Xync deployments can span multiple instances over
   their lifetime. The deployment ID is used as a fixed reference for
   undeploying modules.
* `group` - The HA group to which to deploy the module.
* `module` - The module to deploy.
* `config` - The module configuration.
* `instances` - The number of instances to deploy.
* `ha` - Whether to fail over the deployment if the node fails.

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

vertx.eventBus().send("cluster", message, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      // Deployment was successful.
    }
  }
});
```

All Xync deployments are automatically deployed in HA mode. When multiple
instances of a deployment are specified, *all instances of the deployment
will always be started on the same node* in order to ensure the integrity
of Vert.x's class loader behaviors.

### Undeploying modules
To undeploy modules, simply send an `undeploy` message to the same
`cluster` event bus address. The undeploy message must contain the deployment
ID that was assigned when the deployment was created. When failover occurs,
the assigned deployment ID carries over to the new node and is used as a
reference for undeploying the module.

```java
JsonObject message = new JsonObject()
  .putString("action", "undeploy")
  .putString("type", "module")
  .putString("id", "test");

vertx.eventBus().send("cluster", message, new Handler<Message<JsonObject>>() {
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
Xync provides an event bus based API for distributed shared data.
Essentially, Xync's data support amounts to a simple key-value store,
but it also provides data-driven events, allowing users to watch
keys for changes over the Vert.x event bus.

### Shared maps
The shared map is a cluster-wide map that is accessible over the Vert.x event bus.
The map is backed by a Hazelcast map and has the following operations:

#### put
```
{
  "type": "map",
  "name": "foo",
  "action": "put",
  "key": "bar",
  "value": "baz"
}
```

#### get
```
{
  "type": "map",
  "name": "foo",
  "action": "get",
  "key": "bar",
}
```

#### remove
```
{
  "type": "map",
  "name": "foo",
  "action": "remove",
  "key": "bar"
}
```

#### contains
```
{
  "type": "map",
  "name": "foo",
  "action": "contains",
  "value": "baz"
}
```

#### keys
```
{
  "type": "map",
  "name": "foo",
  "action": "keys"
}
```

#### values
```
{
  "type": "map",
  "name": "foo",
  "action": "values"
}
```

#### empty
```
{
  "type": "map",
  "name": "foo",
  "action": "empty"
}
```

#### clear
```
{
  "type": "map",
  "name": "foo",
  "action": "clear"
}
```

#### size
```
{
  "type": "map",
  "name": "foo",
  "action": "size";
}
```

### Shared lists
The shared list is a cluster-wide list that is accessible over the Vert.x event bus.
The list is backed by a Hazelcast list and has the following operations:

#### add
```
{
  "type": "list",
  "name": "foo",
  "action": "add",
  "value": "bar"
}
```

#### remove
```
{
  "type": "list",
  "name": "foo",
  "action": "remove",
  "value": "bar"
}
```

or

```
{
  "type": "list",
  "name": "foo",
  "action": "remove",
  "index": 2
}
```

#### contains
```
{
  "type": "list",
  "name": "foo",
  "action": "contains",
  "value": "bar"
}
```

#### size
```
{
  "type": "list",
  "name": "foo",
  "action": "size"
}
```

#### empty
```
{
  "type": "list",
  "name": "foo",
  "action": "empty"
}
```

#### clear
```
{
  "type": "list",
  "name": "foo",
  "action": "clear"
}
```

### Shared sets
The shared set is a cluster-wide set that is accessible over the Vert.x event bus.
The set is backed by a Hazelcast set and has the following operations:

#### add
```
{
  "type": "set",
  "name": "foo",
  "action": "add",
  "value": "bar"
}
```

#### remove
```
{
  "type": "set",
  "name": "foo",
  "action": "remove",
  "value": "bar"
}
```

#### contains
```
{
  "type": "set",
  "name": "foo",
  "action": "contains",
  "value": "bar"
}
```

#### size
```
{
  "type": "set",
  "name": "foo",
  "action": "size"
}
```

#### empty
```
{
  "type": "set",
  "name": "foo",
  "action": "empty"
}
```

#### clear
```
{
  "type": "set",
  "name": "foo",
  "action": "clear"
}
```

### Shared queues
The shared queue is a cluster-wide queue that is accessible over the Vert.x event bus.
The queue is backed by a Hazelcast queue and has the following operations:

#### add
```
{
  "type": "queue",
  "name": "foo",
  "action": "add",
  "value": "bar"
}
```

#### remove
```
{
  "type": "queue",
  "name": "foo",
  "action": "remove",
  "value": "bar"
}
```

or without a value, remove the element at the head of the queue
```
{
  "type": "queue",
  "name": "foo",
  "action": "remove"
}
```

#### contains
```
{
  "type": "queue",
  "name": "foo",
  "action": "contains",
  "value": "bar"
}
```

#### size
```
{
  "type": "queue",
  "name": "foo",
  "action": "size"
}
```

#### empty
```
{
  "type": "queue",
  "name": "foo",
  "action": "empty"
}
```

#### clear
```
{
  "type": "queue",
  "name": "foo",
  "action": "clear"
}
```

#### offer
```
{
  "type": "queue",
  "name": "foo",
  "action": "offer",
  "value": "bar"
}
```

#### element
```
{
  "type": "queue",
  "name": "foo",
  "action": "element"
}
```

#### poll
```
{
  "type": "queue",
  "name": "foo",
  "action": "poll"
}
```

#### peek
```
{
  "type": "queue",
  "name": "foo",
  "action": "peek"
}
```
