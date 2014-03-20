package net.kuujo.xync.cluster.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import net.kuujo.xync.cluster.Event;
import net.kuujo.xync.cluster.XyncClusterClient;
import net.kuujo.xync.cluster.Event.Type;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VertxException;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * Default cluster client implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultXyncClusterClient implements XyncClusterClient {
  private static final String CLUSTER_ADDRESS = "__CLUSTER__";
  private final EventBus eventBus;
  private final Map<String, Map<Handler<Event>, HandlerWrapper>> watchHandlers = new HashMap<>();

  private static class HandlerWrapper {
    private final String address;
    private final Handler<Message<JsonObject>> messageHandler;

    private HandlerWrapper(String address, Handler<Message<JsonObject>> messageHandler) {
      this.address = address;
      this.messageHandler = messageHandler;
    }
  }

  public DefaultXyncClusterClient(EventBus eventBus) {
    this.eventBus = eventBus;
  }

  @Override
  public XyncClusterClient deployModule(String deploymentID, String moduleName,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, new JsonObject(), 1, doneHandler);
  }

  @Override
  public XyncClusterClient deployModule(String deploymentID, String moduleName, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, config, 1, doneHandler);
  }

  @Override
  public XyncClusterClient deployModule(String deploymentID, String moduleName, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, new JsonObject(), instances, doneHandler);
  }

  @Override
  public XyncClusterClient deployModule(String deploymentID, String moduleName, JsonObject config,
      int instances, final Handler<AsyncResult<String>> doneHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "deploy")
        .putString("type", "module")
        .putString("module", moduleName)
        .putString("id", deploymentID)
        .putObject("config", config)
        .putNumber("instances", instances);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<String>(result.result().body().getString("id")).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<String>(new VertxException(result.result().body().getString("message"))).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName, Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(deploymentID, group, moduleName, new JsonObject(), 1, doneHandler);
  }

  @Override
  public XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName,
      JsonObject config, final Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(deploymentID, group, moduleName, config, 1, doneHandler);
  }

  @Override
  public XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName,
      int instances, final Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(deploymentID, group, moduleName, new JsonObject(), instances, doneHandler);
  }

  @Override
  public XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName,
      JsonObject config, int instances, final Handler<AsyncResult<String>> doneHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "deploy")
        .putString("type", "module")
        .putString("module", moduleName)
        .putString("group", group)
        .putString("id", deploymentID)
        .putObject("config", config)
        .putNumber("instances", instances);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<String>(result.result().body().getString("id")).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<String>(new VertxException(result.result().body().getString("message"))).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient undeployModule(String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "undeploy")
        .putString("type", "module")
        .putString("id", deploymentID);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<Void>(new VertxException(result.result().body().getString("message"))).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient set(String key, Object value) {
    return set(key, value, null);
  }

  @Override
  public XyncClusterClient set(String key, Object value, final Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "set")
        .putString("key", key)
        .putValue("value", value);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<Void>(new VertxException(result.result().body().getString("message"))).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public <T> XyncClusterClient get(String key, final Handler<AsyncResult<T>> resultHandler) {
    return get(key, resultHandler);
  }

  @Override
  public <T> XyncClusterClient get(String key, Object def, final Handler<AsyncResult<T>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "get")
        .putString("key", key)
        .putValue("default", def);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<T>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<T>((T) result.result().body().getValue("result")).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<T>(new VertxException(result.result().body().getString("message"))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient delete(String key) {
    return delete(key, null);
  }

  @Override
  public XyncClusterClient delete(String key, final Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "delete")
        .putString("key", key);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<Void>(new VertxException(result.result().body().getString("message"))).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient exists(String key, final Handler<AsyncResult<Boolean>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "exists")
        .putString("key", key);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<Boolean>(result.result().body().getBoolean("result")).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<Boolean>(new VertxException(result.result().body().getString("message"))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient watch(String key, Handler<Event> handler) {
    return watch(key, null, handler, null);
  }

  @Override
  public XyncClusterClient watch(String key, Event.Type event, Handler<Event> handler) {
    return watch(key, event, handler, null);
  }

  @Override
  public XyncClusterClient watch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler) {
    return watch(key, null, handler, doneHandler);
  }

  @Override
  public XyncClusterClient watch(final String key, final Type event, final Handler<Event> handler, final Handler<AsyncResult<Void>> doneHandler) {
    final String id = UUID.randomUUID().toString();
    final Handler<Message<JsonObject>> watchHandler = new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        handler.handle(new DefaultEvent(message.body()));
      }
    };

    final HandlerWrapper wrapper = new HandlerWrapper(id, watchHandler);

    eventBus.registerHandler(id, watchHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          final Map<Handler<Event>, HandlerWrapper> handlers;
          if (watchHandlers.containsKey(key)) {
            handlers = watchHandlers.get(key);
          } else {
            handlers = new HashMap<>();
            watchHandlers.put(key, handlers);
          }
          handlers.put(handler, wrapper);
          JsonObject message = new JsonObject().putString("action", "watch").putString("key", key)
              .putString("event", event != null ? event.toString() : null).putString("address", id);
          eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                eventBus.unregisterHandler(id, watchHandler);
                handlers.remove(handler);
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                JsonObject body = result.result().body();
                if (body.getString("status").equals("error")) {
                  eventBus.unregisterHandler(id, watchHandler);
                  handlers.remove(handler);
                  new DefaultFutureResult<Void>(new VertxException(body.getString("message"))).setHandler(doneHandler);
                } else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public XyncClusterClient unwatch(String key, Handler<Event> handler) {
    return unwatch(key, null, handler, null);
  }

  @Override
  public XyncClusterClient unwatch(String key, Type event, final Handler<Event> handler) {
    return unwatch(key, event, handler, null);
  }

  @Override
  public XyncClusterClient unwatch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler) {
    return unwatch(key, null, handler, doneHandler);
  }

  @Override
  public XyncClusterClient unwatch(String key, Type event, final Handler<Event> handler, final Handler<AsyncResult<Void>> doneHandler) {
    final Map<Handler<Event>, HandlerWrapper> handlers = watchHandlers.get(key);
    if (!handlers.containsKey(handler)) {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
      return this;
    }

    JsonObject message = new JsonObject().putString("action", "unwatch").putString("key", key)
        .putString("event", event != null ? event.toString() : null).putString("address", handlers.get(handler).address);
    eventBus.sendWithTimeout(CLUSTER_ADDRESS, message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          HandlerWrapper wrapper = handlers.remove(handler);
          eventBus.unregisterHandler(wrapper.address, wrapper.messageHandler);
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          HandlerWrapper wrapper = handlers.remove(handler);
          eventBus.unregisterHandler(wrapper.address, wrapper.messageHandler, doneHandler);
        }
      }
    });
    return this;
  }

}
