<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Python Security Threat Model

This document describes the detailed security threat model for Apache
Iceberg Python. It is intended for maintainers and automated security triage.

## Purpose

Apache Iceberg Python is primarily a client library and implementation of the
Iceberg table format and catalog interactions for Python applications and
services. It is commonly embedded in larger systems that provide their own
authentication, authorization, and credential management. Because of that
deployment model, many bug classes that look security-relevant in the abstract
are not actually security vulnerabilities in Iceberg Python itself.

This model is intended to answer:

- what Iceberg Python generally treats as a security vulnerability
- what Iceberg Python generally treats as correctness, hardening, or
  deployment work
- which boundaries are primarily owned by Iceberg Python versus the
  surrounding catalog, application, or service
- which issue classes should be downgraded by default by scanners

## Scope

This model is scoped to the Apache Iceberg Python repository itself:

- table format and metadata handling
- catalog and REST catalog clients
- transport, credential, and configuration handling implemented in this repo
- helper tooling shipped in this repo

It is not a general threat model for every Python service that embeds Iceberg
Python.

In particular, it does not attempt to define the complete security model for:

- applications or services that embed Iceberg Python
- storage-level authorization enforced outside Iceberg Python

## Security Goals

Iceberg Python should:

- avoid exposing secrets or delegated credentials to principals that were not
  already trusted with them
- avoid creating new unauthorized capabilities in Iceberg Python-owned
  components
- avoid violating trust boundaries that Iceberg Python itself documents as its
  own

Iceberg Python does not aim to be the primary enforcement point for:

- user-to-user authorization inside the embedding application
- storage-level authorization
- service-side credential scoping performed by an external catalog
- isolation between tenants, principals, or catalogs sharing a single process

That last point is worth stating plainly, because it is the one most often
assumed. Iceberg Python does not guarantee that state is partitioned between
catalog instances, sessions, or tenants within one interpreter. Caches, client
objects, and auth state may be process-wide. A deployment that serves multiple
principals from one process and needs them isolated must provide that
isolation itself — by process, interpreter, or another boundary it controls.
Reports that a process-wide structure is shared across catalogs or tenants are
accepted as bugs worth fixing, not as vulnerabilities.

## Roles

### Operator

The operator configures the surrounding catalog, application, service, and
storage integration around Iceberg Python. This role is trusted to choose
endpoints, warehouses, storage integrations, and credentials.

### Catalog control plane

The catalog control plane resolves tables and supplies metadata, locations,
configuration, and delegated credentials to Iceberg Python. It may be
implemented by a REST catalog server or another catalog implementation.
Iceberg Python assumes this control plane is trusted and outside its primary
security boundary.

### REST catalog client

The REST catalog client consumes catalog-provided metadata, configuration, and
credentials. Client-side bugs in routing, caching, or reuse are real bugs and
worth fixing, but they are security-relevant only where they expose a secret to
an audience outside the process — not merely because state was shared between
catalogs or sessions within it.

### Embedding application

Applications and services embedding Iceberg Python are responsible for their
own user-facing authorization boundaries unless Iceberg Python explicitly
documents otherwise.

### Table writer or maintainer

This role may already have legitimate power to write or replace table
metadata, write or delete files, choose paths under an allowed warehouse or
table location, and invoke destructive maintenance operations. If a report
only shows a new way to achieve the same effect this role can already cause
legitimately, it is usually not a security issue in Iceberg Python.

## Trust Boundaries

### Boundary 1: operator-trusted configuration

The following are generally treated as trusted operator or deployment inputs:

- catalog properties
- endpoint configuration
- warehouse and storage roots
- transport wiring and credential configuration

If a report depends on the attacker controlling those values directly, it is
usually not a vulnerability in Iceberg Python itself.

### Boundary 2: catalog-supplied metadata

Iceberg Python often accepts metadata locations, table properties, namespace
properties, and related control-plane information from a catalog. By default,
Iceberg Python treats those sources as trusted.

This means a malicious catalog supplying incorrect or malicious metadata is
usually not an Iceberg Python vulnerability by itself.

### Boundary 3: REST catalog-supplied configuration and delegated storage access

In REST deployments, Iceberg Python may also accept service endpoints,
configuration, and delegated storage access from the REST catalog server. By
default, those are treated as trusted control-plane inputs unless Iceberg
Python explicitly documents a stronger guarantee.

This means a malicious REST catalog server sending dangerous endpoints is
usually not an Iceberg Python vulnerability by itself. It also means many
credential-selection bugs are often correctness or specification issues rather
than security boundary failures.

The major exception is secret exposure. If Iceberg Python surfaces credentials
or secrets to a new audience that was not already trusted with them, that is
security-relevant.

### Boundary 4: storage-level authorization

Object store permissions are enforced by the storage provider and the
credentials the surrounding deployment chooses to hand to Iceberg Python.
Iceberg Python is not the root authority for bucket- or object-level
authorization.

## In-Scope Security Vulnerabilities

The following categories are generally security-relevant in Iceberg Python
when the report is credible and reproducible.

### 1. Secret or credential disclosure to a new audience

Examples include:

- catalog or storage credentials exposed through a user-visible surface such
  as logs, error messages, or serialized output
- secrets written into a metadata plane readable by a wider audience than the
  one already trusted with them

Sharing of credential-bearing state between catalogs or sessions inside a
single process is **not** covered here; see *Security Goals* on process
isolation.

### 2. Iceberg Python-owned trust-boundary violations

Security issues exist when Iceberg Python is the documented enforcement point
for a boundary and fails to hold it.

The qualifier matters. A boundary belongs to Iceberg Python only where this
model or the project's documentation says it does. Where the surrounding
catalog, service, or deployment is the enforcement point, a client-side gap is
a bug rather than a vulnerability — even when the consequence looks alarming.
In particular, isolation between principals sharing one process is not an
Iceberg Python-owned boundary.

## Usually Out of Scope or Non-Security by Default

These categories may still be real bugs worth fixing, but they are not usually
security vulnerabilities in Iceberg Python itself.

### 1. Correctness bugs

Examples include incorrect metadata handling, ambiguous matching semantics,
and logic bugs that do not create a new trust-boundary violation.

### 2. Parser hardening and malformed-input robustness

Malformed-input crashes, runtime exceptions, and memory amplification are
usually treated as robustness or hardening work rather than security issues in
Iceberg Python itself.

### 3. Malicious catalog or external service scenarios

Reports that require a malicious catalog or other external control-plane
service are usually outside Iceberg Python's primary security boundary.

### 4. Equivalent-harm reports

If the actor already has a legitimate capability that can cause the same harm,
the new path is usually not a security issue.

### 5. Reports requiring an authorized table writer or maintainer

A principal who can already write or replace table metadata, set table
properties, or write and delete table data is operating inside the capability
set described under *Table writer or maintainer*. A report that only shows a
new route to an effect that principal can already produce legitimately is not
a security vulnerability in Iceberg Python.

This covers, by default:

- table properties that change client behavior for readers of that same table
- metadata a writer is already entitled to replace wholesale
- data or delete files a writer is already entitled to add

Containment of what a writer commits — that manifest entries and write paths
stay inside the table's location — is enforced by the catalog, not by this
client. A report showing that Iceberg Python follows a path the catalog
accepted is a hardening opportunity here and a containment question there.

### 6. Resource exhaustion, allocation amplification, and algorithmic complexity

Availability-only findings are not treated as security vulnerabilities in
Iceberg Python by default. This includes:

- out-of-memory conditions from attacker-influenced sizes or counts
- allocation amplification, where a small input drives a large allocation
- decompression bombs and unbounded decode output
- superlinear or exponential algorithmic complexity on attacker-influenced
  input

Iceberg Python is a client library; the surrounding service owns request
admission, resource limits, and process isolation. These reports are accepted
as robustness and hardening work, not as vulnerabilities, unless they also
demonstrate a confidentiality or integrity consequence.

### 7. Out-of-bounds access and native memory-safety findings

Bounds-checking gaps in native or compiled decode paths, including the Cython
Avro decoders, are treated by default as parser hardening rather than as
security vulnerabilities. Crashes, aborts, and read overruns that terminate the
process fall under this rule.

This applies to the root cause, not to a theory about the consequence. A report
that a bounds check is missing is hardening. A report that demonstrates
specific memory contents reaching an attacker-observable channel is a different
report, and is assessed on what it shows.

### 8. Configuration loaded from documented default locations

Iceberg Python loads configuration from documented default locations, including
`.pyiceberg.yaml` resolved from the working directory, the user home directory,
and `PYICEBERG_HOME`, together with `PYICEBERG_`-prefixed environment
variables. This resolution order is public, documented behavior and follows the
same pattern as configuration loading in widely used tooling across the
ecosystem.

A report is not a vulnerability in Iceberg Python merely because it observes
that configuration is discovered this way — for example, that a configuration
file placed in a directory a user later works from will be read when Iceberg
Python runs there. Choosing which directories to execute in, and which
configuration files to trust on the local filesystem, belongs to the operator
and to the surrounding environment, not to this library.

### 9. Provenance of the table being read

**Establishing the provenance of a catalog or table is the reader's
responsibility.** Iceberg Python reads the metadata, manifests, and data files
it is pointed at. It does not authenticate the authorship of that content, does
not treat it as sandboxed input, and does not claim to.

A caller who does not know who wrote a table is in the same position as one
calling an API endpoint whose operator they cannot identify: the decision to
trust the source was made before the library was involved, and the library is
not where it can be recovered.

Publicly published datasets are the common example, but they are only an
example. The same reasoning applies to a `metadata.json` obtained out of band,
a table registered by URL on a user's behalf, a warehouse path an operator
cannot vouch for, or any other route by which content of unestablished
authorship reaches a read. What matters is whether provenance was established,
not how the content arrived.

Reports whose precondition is that provenance was never established are
therefore outside Iceberg Python's primary security boundary by default. Where
a deployment needs that guarantee, it belongs upstream of this library — in the
catalog that vends the table, or in whatever admits a dataset into the
deployment.

## Scanner Calibration Rules

A scanner targeting Iceberg Python should treat a finding as higher-confidence
only if it plausibly shows one of the following:

- exposure of a secret or delegated credential to a new audience
- creation of a new unauthorized capability in an Iceberg Python-owned
  component
- violation of an Iceberg Python-owned trust boundary rather than a
  surrounding catalog, application, service, or operator boundary

A finding should be downgraded or rejected by default if it instead depends
primarily on:

- malformed-input robustness or denial-of-service behavior
- resource exhaustion, allocation amplification, or algorithmic complexity
  with no confidentiality or integrity consequence
- an out-of-bounds access that crashes or aborts without returning
  attacker-observable memory contents
- a malicious catalog or external service, including one that returns
  malicious configuration overrides or credential-vending responses
- a principal that already has equivalent power through legitimate write or
  maintenance capabilities, including an authorized table writer acting within
  their own table
- state shared between catalogs, sessions, or tenants inside a single process
- configuration discovered from a documented default location on the local
  filesystem or from the process environment
- the victim having chosen to load a dataset, table, or catalog published by an
  untrusted party
