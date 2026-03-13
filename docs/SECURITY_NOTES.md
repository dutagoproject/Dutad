# Security Notes

This document is for operators running DUTA services in real environments.

It is not a formal security audit.
It is a practical checklist for reducing avoidable mistakes.

## Core rule

Do not expose more than you need.

For most setups:

- keep daemon admin RPC local-only
- expose mining HTTP only if you need solo public mining

## Default network exposure

Mainnet defaults:

- P2P: `19082`
- daemon RPC: `19083`
- mining HTTP: `19085`

Recommended public exposure:

- public: `19082`
- optional public: `19085`
- keep private: `19083`

## RPC safety

Daemon admin RPC is a powerful local interface.

Treat them like admin sockets:

- bind them to localhost
- avoid reverse proxy exposure unless you know exactly why
- do not open it to WAN by default

## Mining exposure

The mining listener is public-facing by nature.

That means:

- expect malformed requests
- expect stale shares
- expect abusive clients
- monitor logs for repeated reject patterns

Useful things to watch:

- repeated `invalid_address`
- repeated malformed request errors
- sudden spikes in rejected work submissions

## Host security

Minimum good practice:

- keep the host updated
- restrict SSH access
- use firewall rules
- disable anything you are not using
- do not run random services on the same machine

If a host is compromised, assume wallet and service safety are compromised too.

## Suggested firewall posture

Typical public node host:

- allow `19082/tcp`
- allow `19085/tcp` only if running public mining HTTP
- deny or restrict `19083/tcp`

## Logging and monitoring

At minimum:

- watch `journalctl -u dutad -f`

Look for:

- crash loops
- bind failures
- address validation failures
- repeated work rejects

## Beta warning

This release line is still beta.

That means:

- do not treat it like hardened enterprise infrastructure
- test changes before exposing them publicly
- keep backups current
- expect to keep improving deployment practice over time
