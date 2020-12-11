# ScryWarden

ScryWarden is a framework for detecting anomalies in general datasets through the use of behavioral models. The library provides simple interfaces to design behavioral profiles, to pull messages from datasets, and to report on found anomalies. Its also engineered to horizontally scale as needed for larger datasets.

It aims to be an accessible anomaly detection method that doesn't require any advanced knowledge on the topic while also allowing those with relevant knowledge to extend it for their needs.

## Getting Started

Scrywarden requires python 3.6 or greater.

The first thing to do is install the application through pip.

```commandline
$ pip install scrywarden
```

This will install a CLI command in your path called `scrywarden`. Try it out by typing `scrywarden --help`.

```
$ scrywarden --help
Usage: scrywarden [OPTIONS] COMMAND [ARGS]...

  Detects anomalies in datasets using behavioral modeling.

Options:
  -c, --config TEXT  Path to the config file to use.  [default:
                     scrywarden.yml]

  --help             Show this message and exit.

Commands:
  collect      Collect messages to populate behavioral profiles.
  investigate  Investigate current anomalies to find malicious activity.
```

The application depends on PostgreSQL as a database backend. If you use docker, the following docker compose configuration will setup an instance matching the default configurations of the application.

```yaml
version: "3.2"

services:
  postgres:
    image: "postgres"
    ports:
      - "5432:5432"
    environment:
      POSTGRES_PASSWORD: "scrywarden"
      POSTGRES_USER: "scrywarden"
      POSTGRES_DB: "scrywarden"
```

The application is controlled through a configuration file. By default, it looks in your current directory for a file named `scrywarden.yml`. The following is an example of a valid configuration file;

```yaml
database:
  host: "localhost"
  port: 5432
  name: "scrywarden"
  user: "scrywarden"
  password: "scrywarden"

transports:
  heartbeat:
    class: "scrywarden.transport.heartbeat.MixedHeartbeatTransport"

profiles:
  example:
    class: "scrywarden.profile.example.ExampleProfile"
    collector:
      class: "scrywarden.profile.collectors.TimeRangeCollector"
    analyzer:
      class: "scrywarden.profile.analyzers.ExponentialDecayAnalyzer"

shippers:
  csv:
    class: "scrywarden.shipper.csv.CSVShipper"

pipeline:
  queue_size: 500
  timeout: 10
```

Lets break down what each sections does.

### Database Connection

```yaml
database:
  host: "localhost"
  port: 5432
  name: "scrywarden"
  user: "scrywarden"
  password: "scrywarden"
```

This section configures the connection to the PostgreSQL database. These are the default settings when starting scrywarden. Change these to match your database setup if necessary. These can be omitted if the database settings match the defaults.

### Transports

```yaml
transports:
  heartbeat:
    class: "scrywarden.transport.heartbeat.MixedHeartbeatTransport"
```

Transports are one or more classes that are responsible for pulling data from datasets so they can be read by the application. The transports transform the dataset into a series of JSON messages. Each transport is named by its key in the config. This one is named `heartbeat`.

The `class` field defines which python class to use for this transport. This one is using in the builtin `MixedHeartbeatTransport` class. This class sends a series of test messages at a set interval. It defaults to sending the following JSON messages every 5 seconds:

```json
{"person": "George", "greeting": "hello"}
{"person": "Ben", "greeting": "howdy"}
{"person": "Susan", "greeting": "salutations"}
```

The interval and messages can both be configured by the following setup:

```yaml
transports:
  heartbeat:
    class: "scrywarden.transport.heartbeat.MixedHeartbeatTransport"
    config:
      interval: 5
      data:
      - '{"person": "George", "greeting": "Whats up?"}'
      - '{"person": "Melvin", "greeting": "Cool"}'
```

Other transports can be configured the same way by setting config values in the `config` field.

### Profile

```yaml
profiles:
  example:
    class: "scrywarden.profile.example.ExampleProfile"
    collector:
      class: "scrywarden.profile.collectors.TimeRangeCollector"
    analyzer:
      class: "scrywarden.profile.analyzers.ExponentialDecayAnalyzer"
```

This section defines how a behavioral profile detects anomalies. Lets break down the individual parts of this config.

#### Behavioral Profile

```yaml
profiles:
  example:
    class: "scrywarden.profile.example.ExampleProfile"
```

This imports a profile named `example` using the class `scrywarden.profile.example.ExampleProfile`. This is a built in example profile that combines well with the default heartbeat transports.

```python
from scrywarden.profile import fields, Profile, reporters
from scrywarden.transport.message import Message


class ExampleProfile(Profile):
    greeting = fields.Single('greeting', reporter=reporters.Mandatory())

    def matches(self, message: Message) -> bool:
        """Determines if the message matches the profile."""
        return 'greeting' in message

    def get_actor(self, message: Message) -> str:
        """Pulls the actor name from the message."""
        return message.get('person')
```

This behavioral profile outlines a couple of things.

The `matches` method determines if a particular message from a transport matches the behavioral profile. This checks to see if the `greeting` field is in the message. If this profile receives the message `{"person": "George", "greeting": "hello"}` then it would match, but if it was `{"person": "George"}` then it would be skipped.

The `get_actor` pulls out the actor name of the message. Actors are unique identifiers to build behavioral profiles for. In this example, it's pulling out the person who is associated with the greeting. If it receives `{"person": "George", "greeting": "hello"}` then the actor name would be `"George"`.

The class attribute `greeting` defines a feature of the profile that keeps track of the values in the `greeting` field of the each message. For example, if the message `{"person": "George", "greeting": "hello"}` is received, then it tracks it on the backend as:

| actor  | field    | value   | count |
|--------|----------|---------|-------|
| George | greeting | "hello" | 1     |

If it receives the same value again, it increments this count.

| actor  | field    | value   | count |
|--------|----------|---------|-------|
| George | greeting | "hello" | 2     |

If another value comes across, such as `{"person": "George", "greeting": "howdy"}` then it tracks it as a separate feature.

| actor  | field    | value   | count |
|--------|----------|---------|-------|
| George | greeting | "hello" | 2     |
| George | greeting | "howdy" | 1     |

These features are used to calculate the anomaly scores of each incoming message. How these scores are calculated is dictated by the `reporter` defined on the field. The `greeting` field is calculated by the `Mandatory` reporter. This reporter checks the likelihood of the feature value compared to the average of all the other features. So if it received `{"person": "George", "greeting": "hello"}` then it would receive an anomaly score of 0 because the count 2 is higher than the total average of 1.5. If it received a new value like `{"person": "George", "greeting": "What's up?"}` it would receive an anomaly score of 1 because it's never seen it before.

Anomaly scores are always between 0 and 1, with values closer to 1 being more anomalous. These anomaly scores are stored to be analyzed later for malicious activity.

#### Collector

```yaml
profiles:
  example:
    ...
    collector:
      class: "scrywarden.profile.collectors.TimeRangeCollector"
```

This associates the collector `scrywarden.profile.collectors.TimeRangeCollector` class to the `example` profile. The collector determines how anomalies related to the profile are fetched for analysis.

The given collector selects anomalies to analyze over a given time range. By default, it looks at anomalies in periods of one minute chunks. This can be configured with the following configuration:

```yaml
profiles:
  example:
    ...
    collector:
      class: "scrywarden.profile.collectors.TimeRangeCollector"
      config:
        seconds: 30
```

Now it will pull anomalies in 30 second chunks.

#### Analyzer

```yaml
profiles:
  example:
    ...
    analyzer:
      class: "scrywarden.profile.analyzers.ExponentialDecayAnalyzer"
```

This associates the analyzer `scrywarden.profile.analyzers.ExponentialDecayAnalyzer` class to the `example` profile. The analyzer determines if any anomalies collected from the collector are malicious. This analyzer tries to detect large groups of messages with higher anomaly scores while filtering out smaller groups.

### Shippers

```yaml
shippers:
  csv:
    class: "scrywarden.shipper.csv.CSVShipper"
```

Shippers are classes responsible for handling the malicious anomalies found by the analyzers. This one saves the malicious anomalies to a CSV file. By default, it saves it to a file called `alerts.csv` in the current directory. This can be set by modifying the config.

```yaml
shippers:
  csv:
    class: "scrywarden.shipper.csv.CSVShipper"
    config:
      filename: 'anomalies.csv'
```

Now it will save them in a file called `anomalies.csv` in the current directory.

### Pipeline

```yaml
pipeline:
  queue_size: 500
  timeout: 10
```

The pipeline is responsible for passing the messages from the transports to the behavioral profiles. By default it will process messages either when the queue is filled with 500 messages or if 10 seconds have passed since the first message was put in the queue. These can be configured to be different here.

### Start Collecting

With the previous config, messages can start to be collected from the heartbeat transport to the example profile. This is done by running `scrywarden collect`.

```commandline
$ scrywarden collect
INFO Pipeline starting
INFO Sending heartbeat message {'person': 'George', 'greeting': 'hello'}
INFO Sending heartbeat message {'person': 'Ben', 'greeting': 'howdy'}
INFO Sending heartbeat message {'person': 'Susan', 'greeting': 'salutations'}
INFO Sending heartbeat message {'person': 'George', 'greeting': 'hello'}
INFO Sending heartbeat message {'person': 'Ben', 'greeting': 'howdy'}
INFO Sending heartbeat message {'person': 'Susan', 'greeting': 'salutations'}
INFO Processing 6 messages
INFO 6 messages identified between 1 profiles in 0.01 seconds
INFO 3 actors upserted in 0.01 seconds
INFO 3 actors fetched in 0.01 seconds
INFO 0 features fetched in 0.00 seconds
INFO 6 values processed between 1 profiles in 0.08 seconds
INFO 3 features updated in 0.01 seconds
INFO 3 features fetched in 0.00 seconds
INFO 3 messages upserted in 0.01 seconds
INFO 3 events created in 0.00 seconds
INFO 3 event anomalies created in 0.00 seconds
INFO Pipeline process took 0.18 seconds
```

You can see from the logs that the heartbeat went off twice in the 10 second timeout it takes for the pipeline to process messages. After that timeout, it passes the message to the profiles for processing. It does the following steps:

* Identifies and saves the three actors George, Ben, and Susan.
* Pulls the values for each message and checks to see if any matching features exist.
* Generates anomaly scores for the given message values.
* Generates 3 events with 1 associated anomaly each.

Notice how it only generates 3 events instead of 6. This is because after the first loop, the previous features are already recorded, so seeing them again is not anomalous.

Let's run the next step to see how these events are rated based on anomalies.

### Start investigating

Since we now have anomalies in the database, we can analyze them to check for malicious behavior. This is done by running `scrywarden investigate`.

```commandline
$ scrywarden investigate
INFO Creating initial investigation
INFO Fetching events between 2020-12-04 00:30:45.318094 and 2020-12-04 00:31:45.318094
INFO 3 anomalies collected in 0.12 seconds
INFO 3 events assigned to investigation 1 in 0.00 seconds
INFO 3 malicious anomalies found in 0.02 seconds
INFO Investigation completed in 0.20 seconds
INFO Writing 3 anomalies to 'alerts.csv'
```

After checking the CSV, the following is found:

|event_id|message_id                          |actor_id|created_at                |anomaly_id|field_id|score|
|--------|------------------------------------|--------|--------------------------|----------|--------|-----|
|1       |adb85c93-eced-44ba-b4e6-05cae3a98e3e|1       |2020-12-04 00:30:45.318473|1         |1       |1.0  |
|2       |0f015287-3920-4eb5-969a-37b4f65233f8|2       |2020-12-04 00:30:45.318613|2         |1       |1.0  |
|3       |0f015287-3920-4eb5-969a-37b4f65233f8|3       |2020-12-04 00:30:45.318613|3         |1       |1.0  |

The analyzer found that three anomalies were found to be malicious out of three.

## Documentation

Further documentation is coming soon.
