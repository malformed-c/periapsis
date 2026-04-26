# Periapsis — Licensing Manifest

This document explains the reasoning behind Periapsis's licensing choices.

## The core concern

AppGet was an open source Windows package manager built by a single developer. Microsoft read the project, understood the architecture, and shipped winget. The author was left with nothing — not because the license was wrong, but because Apache 2.0 offers no protection against capability replication. A larger organization with more resources can implement the same idea independently and there is nothing a permissive license can do about that. The project was effectively erased.

This is not a commercial concern. There is no intent to build a company around Periapsis or to sell licenses. The concern is simpler: the work should not be absorbed and erased by a party with more resources and distribution power. Periapsis solves a real problem in a non-obvious way. The architecture is documented, the ADRs are public, the benchmarks are reproducible. A well-resourced team could reimplement it in a few months. The license is the only mechanism that makes that politically and legally costly.

## Why not Apache 2.0

Apache 2.0 offers no protection against the AppGet scenario. A cloud provider can read Periapsis, understand the multi-pawn architecture, implement it from scratch, and offer it as a managed Kubernetes service without any obligation to engage with the original project. The original author becomes an outsider in their own ecosystem.

## Why not AGPL

AGPL is designed to close the "SaaS loophole" — if you run AGPL software as a network service you must publish your modifications. This works for software that users directly interact with over a network. It does not work for Periapsis.

Periapsis is a Kubernetes node agent. It talks to the Kubernetes API server, not to end users. A cloud provider running Periapsis internally would never trigger the AGPL network clause. The license would be functionally identical to Apache 2.0 for the threat it is meant to address.

## Why not GPL from the start

GPL is the ideologically coherent choice for infrastructure in the Linux/systemd lineage and the intended final license. But GPL from day one means any organization can study and run Periapsis internally without any obligation to engage with the project or its author. The project becomes available before it has the visibility and institutional recognition that would make it the canonical implementation. That recognition is the real protection — the license is a bridge to get there.

## Why BSL

The Business Source License gives a defined window — until April 26, 2030 — during which production use in managed or hosted services requires engagement with the project. After that date, Periapsis converts to GPL v3.

The Additional Use Grant explicitly prohibits offering Periapsis or a substantially similar system as a hosted or managed Kubernetes service. This directly addresses the AppGet scenario: a cloud provider cannot absorb the capability into a product without being in clear violation. The license does not restrict personal use, internal infrastructure, homelabs, or self-hosted clusters — it restricts only the specific vector through which a larger organization could erase the project's existence.

The Change License is GPL v3, not Apache 2.0. After 2030, Periapsis becomes free software with strong copyleft — anyone who modifies and distributes it must publish their modifications. This fits the systemd lineage the project belongs to and ensures that even after the commercial window closes, the project cannot be quietly forked and closed.

## The CNCF question

CNCF requires Apache 2.0. BSL closes that path while it is in effect. This is an accepted tradeoff — not because CNCF membership is undesirable, but because it requires donating the project to the foundation before the project has the visibility to be the obvious canonical implementation. The sequence matters. If Periapsis becomes recognized as the reference for this architecture, CNCF membership can be revisited with the project already having institutional weight. The license can be changed; the author holds the copyright.

## The systemd angle

Periapsis is architecturally a systemd-family project. It uses systemd-nspawn as its runtime, transient units for pod lifecycle, journald for logging, machinectl for container visibility, and cgroups v2 via systemd's own resource management. It treats Kubernetes as a control plane and delegates everything else to systemd.

Whether this belongs formally in the systemd project is an open question worth pursuing. The Change Date and Change License were chosen with this in mind — GPL v3 is the right license for infrastructure in that lineage, and 2030 gives enough time to understand whether that path is viable before the commercial window closes.

## On authorship

Periapsis was developed with significant assistance from Claude (Anthropic). The architecture, design decisions, debugging, and implementation strategy were directed by the author; Claude served as a highly capable implementation tool in the same way a compiler or IDE does.

The copyright position taken here is that the author is the creative and intellectual originator of the work. This is the practical position most AI-assisted developers take and the one that allows the project to be licensed and distributed at all given the current state of AI copyright law.

## Summary

| Choice | Reason |
|--------|--------|
| BSL not Apache 2.0 | Apache 2.0 offers no protection against capability replication — the AppGet scenario |
| BSL not AGPL | AGPL network clause does not trigger for a Kubernetes node agent |
| BSL not GPL now | Visibility and canonical status must come before the license opens |
| Change License: GPL v3 | Strong copyleft after the window; fits the systemd lineage |
| Change Date: 2030 | Time to establish recognition before the license opens |
| Internal use permitted | The license protects against erasure, not against use |
