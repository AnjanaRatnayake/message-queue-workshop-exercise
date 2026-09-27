# kafka-alo exercise — find the bug with a property-based test

Two Apache Kafka 4.3.1 broker images are already pushed to the Antithesis
registry for your Antithesis tenant, referred to below as `TENANT_NAME`:

| Image | What it is |
| --- | --- |
| `kafka-alo-broker:baseline` | Kafka 4.3.1, unmodified. |
| `kafka-alo-broker:m01` | The same build with **one bug injected**. |

Your job: write a workload and a property that **fails on `m01` and passes on
`baseline`**, and prove it with two Antithesis runs.

That is the whole discipline of property-based testing in one sentence. A
property that fails on both images is not a property of Kafka, it is a bug in
your test. A property that passes on both has not found anything. Only the
pair of runs tells you that you wrote the right property.

## What you are told about the bug

Quite a lot, as it turns out. The exercise is not to guess the bug. It is to
write a workload and a property that catch it, and to prove the property is
fair by running it against the unmodified broker too.

### Kafka in one paragraph

Kafka is a system for storing a stream of messages so other programs can read
them later. Think of a topic as a notebook. Each message is written on the
next blank line, and the lines are numbered from zero. A producer is a program
that writes lines; a consumer reads them in order.

### Copies, and who is in charge

One notebook on one machine is fragile. So Kafka keeps three copies of each
notebook on three different machines, called brokers. One copy is the leader.
Producers write to the leader only. The other two are followers. They
constantly ask the leader "anything new?" and copy the new lines into their
own notebook. Followers that are keeping up are called in-sync replicas, or
the ISR.

If the leader's machine dies, one of the in-sync followers is promoted to
leader and writing continues. That is the whole point of the copies.

### The promise: "acknowledge only when the copies have it"

When a producer writes a line, it can ask Kafka to only say "got it" once the
line is safely on every in-sync copy. That setting is called `acks=all`. Your
workload will use it. The promise is: if the producer heard "got it", the line
will survive a broker dying, because the followers already have it.

How does the leader know the followers have caught up? It tracks a single
number called the high watermark: the line number that every in-sync copy has
reached. If the high watermark is 42, every copy has lines 0 through 41.

### The line of code that keeps the promise

When the leader writes a new line, say line 41, it notes "this request is done
when the high watermark reaches 42", meaning all copies have everything
through line 41. The check reads:

```
if the high watermark >= 42, tell the producer "got it"
```

Until the followers copy line 41, the high watermark stays at 41 and the
producer waits.

### The bug I introduced

I changed the check to:

```
if the high watermark >= 41, tell the producer "got it"
```

One less. Now the leader says "got it" as soon as the copies have line 40.
Line 41, the one just written, is allowed to exist on the leader alone. In
practice the followers copy it a few milliseconds later and most of the time
nobody notices.

**Why it is a believable mistake.** Programmers constantly confuse "the number
of the last line written" (41) with "the number of the next line" (42).
Kafka's own code uses both meanings in different files. Writing `- 1` here
looks reasonable to someone who has the wrong one in their head. It compiles,
tests pass, and a healthy cluster behaves perfectly.

### How it loses data

1. Producer writes line 41. Leader says "got it" before the followers have it.
   The producer trusts this and moves on to line 42.
2. The leader's machine dies right then.
3. A follower is promoted. Its notebook ends at line 40.
4. The producer's line 42 arrives at the new leader and is written as its
   line 41.
5. A reader sees the message that was labelled 40, then the one labelled 42.
   The message labelled 41 is gone, even though Kafka said "got it".

### The cluster you get

3 KRaft nodes, replication factor 3, `min.insync.replicas=2`, unclean leader
election off, auto topic creation off. This is the configuration under which
Kafka genuinely makes the promise above, so it is what makes your property
fair to the unmodified broker. Do not change it.

### Client configuration to use

These are the client settings to use. They are the ones under which the
promise above actually applies, they hold for any client library, and they
keep you from spending the exercise on Kafka client tuning.

**Topics.** Auto-creation is off, so create topics yourself through the admin
API, with 1 partition, replication factor 3, `min.insync.replicas=2`,
`unclean.leader.election.enable=false`, and retention disabled
(`retention.ms=-1`, `retention.bytes=-1`) so nothing you wrote is ever
deleted under you. Give every producer its own topic; it makes "what did I
write here" unambiguous.

**Producer.**

| Setting | Value | Why |
| --- | --- | --- |
| `acks` | `all` | The promise under test. Anything weaker and losing a write on leader failure is allowed behaviour. |
| `enable.idempotence` | `true` | Retries inside the client cannot reorder or duplicate. Any duplicates you see are your own. |
| `linger.ms` | `0` | One record per request. Do not batch. |
| `message.timeout.ms` | `30000` | Bounds how long a send retries internally before it reports failure to you. |
| `request.timeout.ms` | `10000` | Same idea, per request. |

Send **one record at a time** and wait for its acknowledgement before sending
the next. When a send reports failure, the record may or may not be in the
log: **resend the same record, never skip it**. If the client reports a fatal
error (the idempotent producer can), create a new producer and carry on with
the same record.

**Consumer.** Use manual partition assignment (`assign`), not consumer groups
(`subscribe`): no group coordination, no rebalances, and every reader sees the
whole stream. Read from offset 0. Turn off auto-commit and offset storing;
nothing should ever be committed. Set a `group.id` only if your client
insists on one.

Under faults, a fetch or metadata call failing is normal and is **not** a
violation. Only the data you actually read is evidence.

## What you build

Two images, both pushed to the tenant registry under your own tag:

1. **A workload image** `kafka-alo-workload:<yourname>`. Any language. It runs
   as the `workload` service next to the three brokers and must:
   - wait until it can see all three brokers, then emit `setup_complete`
     (via an [Antithesis SDK](https://antithesis.com/docs/reference/sdk/), or
     by writing the JSONL line by hand as described in the
     [setup guide](https://antithesis.com/docs/setup/docker_compose/#add-a-ready-signal-for-fuzzing)),
     then stay alive;
   - ship one or more **test commands** under `/opt/antithesis/test/v1/<suite>/`
     following the [test composer naming rules](https://antithesis.com/docs/product/writing_tests/test_templates/test_composer_reference/):
     `parallel_driver_*` for traffic that can run concurrently under faults,
     `finally_*` for a check that runs once after faults stop, and so on;
   - state its property with SDK assertions (`assert_always`,
     `assert_sometimes`, ...), see
     [Asserting correctness](https://antithesis.com/docs/product/writing_tests/assertions/).
     The report is organised around these, so a test that merely exits
     non-zero is much harder to read.
2. **Two config images**, `kafka-alo-config:<yourname>-m01` and
   `kafka-alo-config:<yourname>-baseline`. Each is a scratch image containing
   only a `docker-compose.yaml`. This directory has both ready to go:
   - `config/` runs the brokers on `kafka-alo-broker:m01` (the bugged image);
   - `config-baseline/` is the same file with the brokers on
     `kafka-alo-broker:baseline` (the control).

   In each, point the `workload` service at your image and leave the brokers
   alone. Every run of your workload against `m01` should be paired with a run
   against `baseline`: the second run is how you tell "I found the bug" from
   "my property is wrong".

## Setup

You need the tenant's registry credential file `TENANT_NAME.key.json` and an
Antithesis API key. Ask the person who gave you this exercise.

```sh
TENANT_NAME=yourtenant

# Registry login (docker or podman)
cat $TENANT_NAME.key.json | docker login -u _json_key https://us-central1-docker.pkg.dev --password-stdin

# snouty: tenant + repository + API key
snouty login --tenant $TENANT_NAME \
  --repository us-central1-docker.pkg.dev/molten-verve-216720/$TENANT_NAME-repository
snouty doctor
```

The [Snouty CLI](https://antithesis.com/docs/ai/snouty/) is the launcher used
below. Everything it does is also available from the tenant's web UI at
`https://TENANT_NAME.antithesis.com`.

## Build your workload

```sh
REG=us-central1-docker.pkg.dev/molten-verve-216720/$TENANT_NAME-repository
ME=yourname

# Point both composes at your workload image (one marked line in each)
sed -i "s/kafka-alo-workload:YOURNAME/kafka-alo-workload:$ME/" config/docker-compose.yaml config-baseline/docker-compose.yaml

# Build it under exactly the bare name the compose now uses
docker build -t kafka-alo-workload:$ME ./workload
```

## Validate locally

An hour-long run that never emitted `setup_complete`, or whose test commands
were mis-named, tells you nothing. `snouty validate` catches both in a few
minutes: it brings the compose stack up on your machine, waits for
`setup_complete`, then scans `/opt/antithesis/test/v1` in every container and
checks the test-command names. It does not run the commands.

It never pulls images, so every `image:` in the compose must already exist
locally under exactly that bare name. Your workload already does if you built
it as above; pull the broker image from the registry and re-tag it:

```sh
docker pull $REG/kafka-alo-broker:m01
docker tag  $REG/kafka-alo-broker:m01 kafka-alo-broker:m01

snouty validate config --timeout 180
```

Notes:

- The 3-node cluster takes longer than the default 120 s to form on a cold
  start, hence `--timeout 180`.
- Validate one directory at a time. `config/` and `config-baseline/` use the
  same `container_name`s, so they cannot both be up on one host. Validating
  `config/` is enough; the two composes differ only in the broker tag.
- `snouty validate config --keep-running` leaves the stack up afterwards, so
  you can run a test command by hand and watch it against a healthy cluster:

  ```sh
  docker compose -f config/docker-compose.yaml exec workload /opt/antithesis/test/v1/<suite>/parallel_driver_<name>
  docker compose -f config/docker-compose.yaml down -v
  ```

  Without faults, your `assert_always` properties should all hold here on
  both broker images. If one fails locally, the property is wrong, not Kafka.
- Validate also renders the compose twice, once with your shell environment
  and once scrubbed the way Antithesis runs it, and fails if any `${VAR}`
  differs. The starter composes use no variables. Keep it that way.

## Push and launch

```sh
# 1. Workload image, under its registry name
docker tag  kafka-alo-workload:$ME $REG/kafka-alo-workload:$ME
docker push $REG/kafka-alo-workload:$ME

# 2. Config images: bugged brokers from config/, control from config-baseline/
docker build -t $REG/kafka-alo-config:$ME-m01      config
docker push        $REG/kafka-alo-config:$ME-m01
docker build -t $REG/kafka-alo-config:$ME-baseline config-baseline
docker push        $REG/kafka-alo-config:$ME-baseline

# 3. Launch both. Nothing is built or pushed at this point; Antithesis pulls
#    every image named in the compose from the registry.
snouty launch --webhook basic_test --duration 60 --ephemeral \
  --config-image $REG/kafka-alo-config:$ME-m01 \
  --source $ME-m01 --test-name "$ME kafka-alo m01" \
  --description "exercise: bugged broker"

snouty launch --webhook basic_test --duration 60 --ephemeral \
  --config-image $REG/kafka-alo-config:$ME-baseline \
  --source $ME-baseline --test-name "$ME kafka-alo baseline" \
  --description "exercise: unmodified broker"
```

Bare image names in the compose (`kafka-alo-broker:m01`,
`kafka-alo-workload:$ME`) resolve against the tenant registry, which is why
the broker images never have to exist on your machine for a launch. Keep
`--source` different for the two runs so their property histories do not mix,
and keep `--ephemeral` so exercise runs do not pollute the tenant's real
history.

Sixty minutes is enough for a workload that exercises the right thing. If your
first `m01` run comes back clean, look at whether your traffic was actually in
flight while faults were happening before you reach for a longer run.

## Reading the results

```sh
snouty runs list -n 5
snouty runs properties --failing <run-id>
snouty runs show <run-id> --web
```

You are done when the `m01` run shows your property **failing** and the
`baseline` run shows the same property **passing** with a healthy number of
examples. Also check that your `assert_sometimes` properties were reached: if
the "interesting thing happened" property never fired on either run, the
workload did not exercise the situation the bug needs, and a passing
`baseline` proves less than it looks like.

If `baseline` fails too, your property is stronger than what Kafka promises
under this configuration. That is the more instructive failure. Work out which
promise you assumed that Kafka never made.

## Rules

- Do not change the broker services in either compose. Apart from the project
  name, the broker image tag is the only line on which `config/` and
  `config-baseline/` differ. It must stay that way for the pair of runs to
  mean anything.
- You know the bug. You do not get the reference workload that first caught
  it until you have two runs that disagree. Compare notes afterwards.

## Layout

| Path | What it is |
| --- | --- |
| `config/` | Image-only compose with the brokers on `kafka-alo-broker:m01` (bugged), plus the two-line scratch `Dockerfile` that turns it into a config image. Edit the one marked workload line. |
| `config-baseline/` | The same, with the brokers on `kafka-alo-broker:baseline` (control). Your property must pass here. |
| `workload/` | Yours to create. |
