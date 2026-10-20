# Python Design Patterns

Small Python examples of behavioral and architectural patterns. Each directory is a standalone project with its own `pyproject.toml`; these examples are for learning and are not intended as production-ready frameworks.

The projects require Python 3.14 or newer and use [uv](https://docs.astral.sh/uv/) for environment and dependency management. From a pattern directory, install its environment with:

```sh
uv sync
```

Run the example with `uv run ...` from that directory. The sections below link to each pattern's entry point and the code that implements its central idea.

## Command

The Command pattern packages an operation and the information it needs into an object. A caller can record and manage operations without knowing their implementation details.

In [command](command/), bank transactions implement a shared transaction protocol. Deposits, withdrawals, transfers, and batches are stored in a ledger. The controller maintains a cursor over that ledger, so moving the cursor supports undo and redo; balances are recomputed by replaying the active transactions. A new transaction after undo discards the redo branch. `Batch` demonstrates composing commands, though it executes them in order rather than rolling back earlier steps if one fails.

Start with [the demo](command/main.py), then see [transaction implementations](command/banking/commands.py) and [ledger control](command/banking/controller.py).

## Event Sourcing

Event Sourcing stores changes as an append-only sequence of events and derives current state by replaying that history. It makes the record of what happened the source from which a current view is built.

In [event_sourcing](event_sourcing/), inventory additions and removals are events held in an in-memory event store. The inventory view replays them to calculate item counts, while separate projections summarize additions by item and origin. This demonstrates how multiple read views can be derived from one event stream. The store is in-memory, and the sample projections count additions rather than net inventory after removals.

Explore [the demo](event_sourcing/main.py), [event definitions](event_sourcing/event.py), [inventory replay](event_sourcing/inventory.py), and [projections](event_sourcing/projections.py).

## Observer

The Observer pattern lets a subject notify interested observers when it has something to report, without directly depending on each observer's behavior. It is useful when one event should fan out to multiple listeners.

[observer](observer/) shows two forms of the same idea: a subject with attach/detach operations and observer objects, and a list of callback functions invoked by a notifier. The examples send a message to multiple listeners. Run [the object-oriented example](observer/observer_class.py) or [the callback example](observer/observer_func.py) directly; [the package entry point](observer/main.py) currently only prints a greeting.

## Registry and Plugins

A registry maps names to implementations so callers can discover or select behavior without hard-coding every implementation at the call site. A plugin system extends this idea by loading implementations from modules.

[registry_plugins](registry_plugins/) demonstrates both approaches. The CLI discovers command and plugin modules, whose decorators register handlers as a side effect of importing them. The resulting Typer application exposes text commands such as count, reverse, and shout. Separately, [basic_registry.py](registry_plugins/basic_registry.py) shows a small name-to-exporter registry for PDF, CSV, and JSON formats.

The CLI's discovery is limited to its configured package directories, and registration depends on importing the plugin modules. See [the CLI entry point](registry_plugins/main.py), [the shared registry](registry_plugins/registry.py), and [an example plugin](registry_plugins/plugins/shout.py).

## Specification

The Specification pattern represents a rule as a composable object or predicate. Rules can be combined to express richer eligibility or filtering logic while keeping each condition independently understandable.

### Predicate rules

In [specification/spec_pattern](specification/spec_pattern/), user eligibility checks are assembled from predicates using `&` (and), `|` (or), and `~` (not). The examples include both parameterized rule factories and fixed predicates. See [the demo](specification/spec_pattern/main.py) and [rule decorators and composition](specification/spec_pattern/rules.py).

### Composable pipelines

[specification/pipelines](specification/pipelines/) applies composition to units of work. The synchronous `Runnable` API chains steps with `|`, binds arguments, branches between runnables, and combines outputs into a dictionary with a parallel-style runnable. The parallel example currently invokes its tasks sequentially; its focus is composition, not concurrent execution. Decorator variants are shown in [factory_decorator.py](specification/pipelines/factory_decorator.py) and [multiparam_decorator.py](specification/pipelines/multiparam_decorator.py).

The separate [async pipeline](specification/pipelines/async_pipeline.py) composes asynchronous steps with `|` and uses `&` for a truthiness-gated continuation. These examples illustrate a related composition technique rather than the predicate rules above. Start with [the synchronous pipeline demo](specification/pipelines/main.py).

## State

The State pattern makes behavior depend on an object's current state and models valid changes explicitly. A transition table is particularly useful when the allowed lifecycle is clearer as state/event pairs than as scattered conditionals.

In [state](state/), a generic state machine maps a current state and event to a next state and action. The payment example registers transitions such as new to authorized to captured to refunded, along with failure paths. An invalid state/event pair raises `InvalidTransition`; transition actions and the audit history are held in memory.

See [the payment demo](state/main.py) and [the reusable state machine](state/state_machine.py).

## Template Method

Template Method defines the outline of an algorithm in a base class while allowing subclasses to customize selected steps. The shared workflow remains consistent even when individual decisions differ.

In [template_method](template_method/), `TradingBot` connects to an exchange, fetches prices, and then calls subclass hooks to decide whether to buy or sell. `AverageTrader` and `MinMaxTrader` provide different price rules. The exchange is injected separately, so the example also illustrates separating the data source from the trading algorithm.

See [the demo and concrete strategies](template_method/main.py).

## Webhooks

[webhooks](webhooks/) evolves a short-link API through three ways of notifying external consumers. Comparing the versions shows how event delivery can be separated from the operation that produces an event.

- **V1: links only.** Create and list short links, redirect by code, and increment a click count. There is no webhook delivery. See [V1](webhooks/V1/main.py).
- **V2: direct delivery.** Link creation and clicks send payloads to registered webhook URLs. The API calls the delivery code directly, which keeps the flow simple but couples request handling to synchronous HTTP delivery. See [V2](webhooks/V2/main.py) and [the webhook registry](webhooks/V2/webhooks.py).
- **V3: event bus.** Link operations publish typed events, and webhooks subscribe to selected event types through a shared event bus. Producers no longer need to know which webhooks consume each event. Delivery is still synchronous in this example. See [V3](webhooks/V3/main.py), [event definitions and bus](webhooks/V3/events.py), and [subscriptions](webhooks/V3/webhooks.py).

All three versions use in-memory data. The V2 and V3 deliveries are illustrative rather than production-grade: they have no retry or durable queue, and a delivery failure can affect the request or event dispatch. The [webhook receiver](webhooks/webhook_receiver.py) is a small local endpoint for trying the flow. Setup and run instructions are in [webhooks/README.md](webhooks/README.md).
