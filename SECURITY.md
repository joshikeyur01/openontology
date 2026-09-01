# Security Policy

## Reporting a vulnerability

**Do not open a public issue.**

Report privately to **keyurjoshi2104@gmail.com**, or through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository.

Please include:

- what the issue is and which component it affects
- steps to reproduce, or a proof of concept
- the impact you think it has
- any configuration required to trigger it

You will get an acknowledgement within 72 hours and an assessment within 7 days.
If the report is valid you will be told when a fix lands and credited in the
advisory unless you would rather not be.

## Scope

Vulnerabilities in this repository's own code are in scope, including the
Apache-2.0 and BUSL-1.1 halves equally.

Out of scope: vulnerabilities in upstream dependencies or container images
(report those upstream — tell us if OpenOntology's use makes one materially
worse), and the deliberate deployment limitations below.

## Deliberate limitations, not vulnerabilities

The default configuration is a local development topology and is not hardened
for production or for exposure to a network. These are known and documented,
not findings:

- **No transport encryption.** Kafka is `PLAINTEXT`, Redis is unauthenticated,
  Neo4j uses Bolt without TLS. Everything binds to `localhost` and shares a
  private compose network.
- **Demo subscription keys are public.** The four keys in `.env.example` exist
  so every branch of the licensing path is demonstrable. They are not secrets
  and must be replaced via `OO_LICENSE_REGISTRY_PATH` for any real deployment.
- **The default Neo4j password is `openontology`**, and it is in the example
  environment file for the same reason.
- **No authentication on operational endpoints.** `/stats`, `/metrics` and
  `/readyz` on ports 8081 and 8082, and the operator console, are unauthenticated
  and read-only. They disclose asset identifiers, counters and configuration.
  Do not expose them publicly.
- **Single-node everything.** One Kafka broker at replication-factor 1, one
  Redis with no replica, one Neo4j community node with no backup.

A report that the default compose file is insecure to expose to the internet
will be closed as documented behaviour. A report that one of these limits can be
escaped — say, that a licence check can be bypassed, or that the open core can
be made to execute something a caller controls — is very much in scope.

## What is treated as a real finding

- Bypassing the licensing middleware: reaching `/v1/intercept` without a valid
  entitled subscription, or causing quota not to be counted.
- Cross-tenant leakage: one tenant reading another's stored plans, quota state
  or twin data.
- Injection into the model call that escapes the guardrails — in particular
  anything that gets an irreversible action (`ISOLATE`, `SHUTDOWN`,
  `EMERGENCY_SHUTDOWN`) into a validated plan when the server-side checks should
  have downgraded or rejected it.
- Any path where telemetry content controls execution: command injection through
  an asset or sensor identifier, Cypher injection through a graph lookup, or
  deserialisation of untrusted payloads into executable state.
- Denial of service reachable from a single well-formed request, as opposed to
  volume.

## Supported versions

`main` is the supported branch. There are no long-lived release branches yet;
fixes land on `main` and are tagged.
