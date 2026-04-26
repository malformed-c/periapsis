# Periapsis — Licensing Manifest

This document explains the reasoning behind Periapsis's licensing choices.

## Why not Apache 2.0

Apache 2.0 is the lingua franca of the cloud-native ecosystem — CNCF requires it, most Kubernetes tooling uses it, and it signals openness to enterprise adopters. It was also the license that did nothing for AppGet.

AppGet was an open source Windows package manager. Microsoft read the project, understood the architecture, and shipped winget. The author had no recourse — not because the license was wrong, but because Apache 2.0 offers no protection against capability replication. A larger organization with more resources can implement the same idea independently and there is nothing a permissive license can do about that.

Periapsis solves a real problem in a non-obvious way. The architecture is documented, the ADRs are public, and the benchmarks are reproducible. A well-resourced team could reimplement it. Apache 2.0 would make that straightforward.

## Why not AGPL

AGPL is designed to close the "SaaS loophole" — if you run AGPL software as a network service you must publish your modifications. This works for software that users directly interact with over a network. It does not work for Periapsis.

Periapsis is a Kubernetes node agent. It talks to the Kubernetes API server, not to end users. A cloud provider running Periapsis internally to provide cheaper pod scheduling to their customers would never trigger the AGPL network clause. The license would be functionally identical to Apache 2.0 for the threat it is meant to address.

## Why not GPL from the start

GPL would be the ideologically coherent choice for infrastructure in the Linux/systemd lineage. systemd itself is LGPL. The kernel is GPL. A native systemd Kubernetes node agent fits that world.

But GPL from day one means any company can study, fork, and run it internally without any obligation to engage. The commercial window disappears. If a large organization decides Periapsis's approach is worth pursuing, GPL gives them full freedom to build on it without any reason to talk to the original author.

## Why BSL

The Business Source License gives a defined commercial window — until April 26, 2030 — during which production use in managed or hosted services requires a commercial license. After that date, Periapsis converts to GPL v3.

This accomplishes three things:

**It closes the managed service vector.** The Additional Use Grant explicitly prohibits offering Periapsis or a substantially similar system as a hosted or managed Kubernetes service. This is the specific threat Apache 2.0 cannot address — a cloud provider absorbing the capability into a product without engaging with the project.

**It preserves internal use.** Running Periapsis on your own infrastructure, whether bare metal, VPS, or cloud VMs you control, is explicitly permitted at no cost. The license is not designed to extract money from users — it is designed to prevent the project from being commoditized by a party with distribution advantages.

**It converts to strong copyleft.** GPL v3 on the Change Date means that after the commercial window closes, anyone who modifies and distributes Periapsis must publish their modifications. The project does not become a free resource for proprietary forks after 2030.

## The CNCF question

CNCF requires Apache 2.0. BSL closes that path as long as it is in effect.

This is an accepted tradeoff. CNCF membership requires donating the project to the foundation — surrendering the commercial control that BSL is designed to protect. The two goals are in direct tension. Projects that have tried to hold both (see: HashiCorp/Terraform) have found the community responds poorly to the switch.

If the project reaches a point where CNCF donation makes sense — because institutional belonging is more valuable than the commercial window — the license can be changed. The author holds the copyright and can relicense.

## The systemd angle

Periapsis is architecturally a systemd-family project. It uses systemd-nspawn as its runtime, transient units for pod lifecycle, journald for logging, machinectl for container visibility, and cgroups v2 via systemd's own resource management. It treats Kubernetes as a control plane and delegates everything else to systemd.

Whether this belongs formally in the systemd project is an open question. The Change Date and Change License were chosen with this in mind — GPL v3 is the right license for infrastructure that aspires to that lineage, and 2030 gives enough time to understand whether that path is viable.

## On authorship

Periapsis was developed with significant assistance from Claude (Anthropic). The architecture, design decisions, debugging, and implementation strategy were directed by the author; Claude served as a highly capable implementation tool.

The copyright position taken here is that the author is the creative and intellectual originator of the work, and AI assistance is a tool in the same category as a compiler or an IDE. This is the practical position most AI-assisted developers take, and it is the position that allows the project to be licensed and distributed at all given the current state of AI copyright law.

## Summary

| Choice | Reason |
|--------|--------|
| BSL not Apache 2.0 | Apache 2.0 offers no protection against capability replication |
| BSL not AGPL | AGPL network clause does not trigger for a Kubernetes node agent |
| BSL not GPL now | GPL from day one removes all commercial leverage before the project has visibility |
| Change License: GPL v3 | Strong copyleft after the commercial window; fits the systemd lineage |
| Change Date: 2030 | Four years to establish canonical status before the license opens |
| Internal use permitted | The license protects against commoditization, not against use |
