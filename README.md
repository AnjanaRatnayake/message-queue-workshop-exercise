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

- It is in the **broker**, not in any client library, and not in the KRaft
  controller or the storage layer.
- It concerns **what the broker promises a client** about a write. Under an
  ordinary sequence of events the promise is kept. Under an unlucky sequence
  of failures, it is not.
- It needs a **fault** to become visible: no amount of load against a healthy
  cluster will trigger it. Antithesis injects faults for you (broker kills,
  pauses, network partitions, clock skew). Your workload has to be running the
  right kind of traffic when the fault lands, and has to be able to *notice*
  afterwards.
- The cluster is 3 KRaft nodes, replication factor 3, `min.insync.replicas=2`,
  unclean leader election off, auto topic creation off. Every one of those
  matters to the property you end up writing. Do not change them.

Think about which guarantees Kafka makes to a producer or consumer under that
configuration, which of them a client could actually *check* from the outside,
and what evidence you would need to have recorded beforehand to check it.

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
- Do not go looking for the patch or the reference workload that produced these
  images until you have two runs that disagree. Compare notes afterwards.

## Layout

| Path | What it is |
| --- | --- |
| `config/` | Image-only compose with the brokers on `kafka-alo-broker:m01` (bugged), plus the two-line scratch `Dockerfile` that turns it into a config image. Edit the one marked workload line. |
| `config-baseline/` | The same, with the brokers on `kafka-alo-broker:baseline` (control). Your property must pass here. |
| `workload/` | Yours to create. |
