# Docker Desktop Image Pull Failure – Troubleshooting Report (macOS)

## 1. Issue Summary

**Problem**  
Docker Desktop on macOS was unable to pull images from Docker Hub. Commands such as:

```bash
docker pull hello-world
docker pull busybox:latest
docker pull grafana/grafana:10.2.0
```

either produced no output or failed with errors such as:

- `context deadline exceeded`
- `connect: connection refused`
- `Docker Desktop has no HTTPS proxy`

As a result, `docker compose up -d` could not start services.

---

## 2. Error Symptoms (Key Messages)

Representative errors observed:

```text
failed to resolve reference "docker.io/library/busybox:latest"
dial tcp <IP>:443: connect: connection refused
container via direct connection because Docker Desktop has no HTTPS proxy
```

Key diagnostic signal:

> Docker explicitly stated it was attempting a **direct connection** and had **no HTTPS proxy configured**.

---

## 3. Environment Context

- **OS**: macOS  
- **Docker**: Docker Desktop (4.x)  
- **Network tool**: Clash Verge  
- **VPN mode**: System proxy (non-TUN)  
- **Docker runtime**: Linux VM (Docker Desktop internal)

---

## 4. Root Cause Analysis

### Primary Root Cause
**Clash Verge was enabled, but Docker Desktop traffic was not routed through it.**

### Why this happened
- Clash Verge (without TUN mode) proxies **macOS application traffic only**
- Docker Desktop runs containers inside a **Linux VM**
- VM traffic **bypasses system proxy**
- Direct outbound HTTPS traffic to Docker Hub was blocked
- Docker therefore failed with `connection refused`

This is expected behavior and **not a Docker bug**.

---

## 5. Investigation Steps (Condensed)

1. Verified Docker daemon status
2. Reproduced failure with minimal images
3. Identified missing HTTPS proxy in Docker
4. Confirmed Clash Verge was active
5. Identified Clash **Mixed Port = 7897**
6. Configured Docker Desktop to use that proxy

---

## 6. Final Resolution (Successful Fix)

### Docker Desktop Configuration

**Path**  
Docker Desktop → Settings → Resources → Proxies

**Final working settings**:

```text
Manual proxy configuration: Enabled
HTTP Proxy:  http://127.0.0.1:7897
HTTPS Proxy: http://127.0.0.1:7897
Bypass:      localhost,127.0.0.1,.local
```

### Why this works
- Port **7897** is the active Clash Verge Mixed Port
- Docker traffic is tunneled through Clash
- Direct outbound connections are avoided

---

## 7. Verification Results

```bash
docker pull hello-world
docker pull busybox:latest
docker pull grafana/grafana:10.2.0
docker pull prom/prometheus:v2.48.0
```

All commands completed successfully.

---

## 8. Lessons Learned / Best Practices

1. Docker Desktop does not inherit VPN/proxy automatically
2. With Clash:
   - Enable **TUN mode**, or
   - Configure Docker proxy explicitly
3. Errors mentioning **no HTTPS proxy** indicate routing issues
4. Reinstalling Docker does not fix network-path problems

---

## 9. Recommended Standard Setup

- **Preferred**: Clash TUN mode + no Docker proxy
- **Alternative**: Clash system proxy + Docker manual proxy (this case)

---

## 10. Final Status

- **Status**: Resolved
- **Result**: Docker, Compose, Grafana, Prometheus fully operational
- **Note**: Update Docker proxy if Clash port changes
