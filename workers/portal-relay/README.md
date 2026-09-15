# portal-relay

Gives the weekly refresh an unblocked hop to the two inspection portals that
refuse GitHub Actions.

## Why

`inspections.myhealthdepartment.com` answers 403 to every GitHub-hosted
runner range (ubuntu, macOS and windows alike), so the scheduled refresh
silently shipped 0 records for DFW, the Portland metro, the Colorado Front
Range, Utah County and Yolo, week after week, while still reporting success.
The Southern Nevada Health District API has failed from runners too.

The hop does not have to be a paid residential proxy: ordinary datacenter IPs
are not blocked. An ordinary Google Cloud host reached both portals fine on
2026-09-15, search and detail pages, no 403. This Worker runs on the
Cloudflare account that already serves the site.

## Read this before deploying

The portal blocks on **two** things independently, measured from that one
host in the same minute:

| Client | Result |
|---|---|
| Python `requests` (what the pipeline uses) | 200 |
| `curl` (OpenSSL, HTTP/2) | 200 |
| Node `fetch` / undici | **403**, regardless of headers or accept-encoding |

So it is not only the egress IP — the client's TLS fingerprint matters too,
and at least one major non-OpenSSL client is already refused. A Cloudflare
Worker's `fetch` is not an OpenSSL client either, so **this Worker may well
get the same 403 undici does**. It is cheap to deploy and one command to
test, which is why it is here, but it is a bet, not a fix.

The proven path is running this pipeline's own Python from any non-GitHub
host: a self-hosted runner, or a small VM on a cron. That clears both
conditions by construction.

## Deploy

```
cd workers/portal-relay
npx wrangler deploy
npx wrangler secret put RELAY_TOKEN      # paste a long random string
```

Generate the token with `openssl rand -hex 32`. The Worker refuses every
request until `RELAY_TOKEN` is set, so it is never briefly an open relay.

Then add two repository secrets (Settings → Secrets and variables → Actions):

| Secret | Value |
|---|---|
| `PORTAL_RELAY_URL` | the deployed Worker URL, e.g. `https://dinescores-portal-relay.<subdomain>.workers.dev` |
| `PORTAL_RELAY_TOKEN` | the same string you gave `wrangler secret put` |

## Verify before trusting it

Cloudflare's egress is a different network from GitHub's, but whether these
portals accept it has NOT been confirmed from a deployed Worker. Check it
before assuming the weekly job is fixed:

```
curl -s -o /dev/null -w '%{http_code}\n' \
  -H "X-Relay-Token: $TOKEN" \
  "$WORKER_URL?url=https%3A%2F%2Finspections.myhealthdepartment.com%2F"
```

`200` means the portal accepts Cloudflare and the next scheduled refresh will
pick DFW back up. `403` means Cloudflare's ranges are blocked too, and the
fallback is a host on an unblocked network: a self-hosted runner, a small VM
running the pipeline on a cron, or a residential proxy in `PORTAL_PROXY_URL`.

The `probe-sources` workflow runs the same check across every portal when
`PORTAL_RELAY_URL` is set.

## How it works

A Worker cannot serve HTTP CONNECT, so this is not an HTTP proxy and does not
go in `PORTAL_PROXY_URL`. The pipeline puts the target URL in a `url` query
parameter (`_relay()` in `dinescores_pipeline.py`) and the Worker fetches it,
returning the portal's own status and body untouched. That matters: the
pipeline's 403/429 retry ladders, the run-wide circuit breaker and the
detail-ban cooldowns all read those status codes and keep working unchanged.

Only the two portal hostnames are fetchable (`ALLOWED_HOSTS`). Without that
allowlist anyone who learned the URL would have a general-purpose open proxy.
