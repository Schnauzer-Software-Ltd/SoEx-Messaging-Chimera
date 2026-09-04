# AI Provenance

This repository contains code and documentation produced with AI assistance
(Anthropic Claude).

## Boundary

The SoEx core libraries are a separate, human-authored project. Code from this
repository must not be copied, merged, or otherwise contributed into the SoEx
core repository. This project integrates with SoEx through its published package
API only.

This marker exists so the origin of code in this repository is unambiguous and
the boundary with SoEx core is auditable.

## Note on the durability claims

This library persists messages to local SQLite files and makes explicit
durability, retention and delivery claims. Those claims are the product, so the
tests that establish them run against the real engine rather than a substitute,
and the limits are stated in the README rather than left implied.
