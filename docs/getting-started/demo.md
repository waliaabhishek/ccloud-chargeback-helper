# Try the demo

Explore Chitragupta or give a presentation without connecting a provider account.
The demo includes six months of synthetic Confluent Cloud and self-managed Kafka
data, with resources, identities, tags, and allocated costs ready to explore.
No provider credentials are needed, and the demo does not fetch billing or usage
from external systems.

## Watch the walkthrough

Follow a cost increase from the dashboard to a Kafka topic, compare daily
amounts, and inspect processing status and reporting options. Press play to
watch the investigation using synthetic data.

<video controls playsinline preload="none" width="960" style="width: 100%; height: auto;" poster="../../assets/demo/chitragupta-demo-dashboard-poster.webp" aria-label="Chitragupta demo walkthrough">
  <source src="../../assets/demo/chitragupta-demo-walkthrough.mp4" type="video/mp4">
  Your browser does not support embedded video.
  <a href="../../assets/demo/chitragupta-demo-walkthrough.mp4">Download the walkthrough (MP4)</a>.
</video>

## Start the demo

You need Git and Docker with Compose, with Docker running. The first startup
requires internet access to download container images.

```bash
git clone https://github.com/waliaabhishek/chitragupta.git
cd chitragupta
./demo --showcase
```

Wait for startup to finish, then open **<http://127.0.0.1:8081>**. The API is
available at <http://127.0.0.1:8080>. Both are accessible only from your machine
by default.

Choose a profile for the experience you want:

| Profile | Use it for | Start command |
|---|---|---|
| Showcase | Presentations and investigating deliberately introduced cost changes | `./demo --showcase` |
| Clean | Exploring healthy, fully allocated data without deliberately introduced issues | `./demo` |

Both profiles include Confluent Cloud and self-managed Kafka tenants. Use the
tenant selector at the top right to switch between them. Profile data is kept
separately, so switching profiles preserves changes you made in each one.

## Try a cost investigation

Start with Showcase and select **clean-confluent** in the tenant selector.
The tenant name stays the same in both profiles.

1. Open **Dashboard** and use **Overview** to confirm the tenant and ordinary
   filters. Select **Compare**, then choose **Previous day**, **Previous week**,
   **Calendar month**, or **Custom**. Review the baseline and comparison totals,
   the largest changes, and the selected timezone. **Movement** filters the
   table; **Rows** limits the returned top-N groups. The summary remains the
   full filtered scope, while Reconciliation shows groups excluded by movement
   and groups outside top N, including their baseline, comparison, and net
   amounts.
2. Select a chargeback row's **Compare entity in Cost Explorer** action to
   continue with both periods and the comparison timezone. Explorer does not
   carry product, cost-type, or tag filters, and it shows the entity in its
   broader tenant context.
3. Open **Topic Attribution** and select **Compare** when the source is enabled.
   For Showcase data, set **Cluster** to `lkc-customer` and **Topic Name** to
   `showcase-live-orders`. Its totals are attributed Kafka costs, not the full
   tenant bill. Choose Topic or Cluster, apply the existing topic filters, and
   use the same movement, sorting, and row-limit controls. **Open filtered Topic
   Attribution list** returns to Table with the selected period and filters; the
   existing list uses substring matching, so another containing match may
   appear.
4. Read the coverage qualification before interpreting a zero. Unknown or
   incomplete dates make the amounts observed totals and prevent a confirmed
   zero claim. A zero baseline makes percentage change unavailable. If demo data
   is outside the selected range, choose Custom with dates containing the data.
5. For monthly tenants, Compare uses UTC, disables Previous day and Previous
   week, and requires each Custom period to start on the first day and end on the
   last day of a complete UTC calendar month. Daily and hourly comparisons accept
   an IANA timezone; daylight-saving transitions can make the two elapsed
   durations differ even when the date ranges look alike.
6. Open **Pipeline** to check processing status. For reporting, open **FOCUS
   Mapping Preview** to inspect the available monthly previews and download
   options.

Cost Explorer's ordinary view shows a snapshot for its selected date. The
**Compare entity in Cost Explorer** handoff carries both comparison periods and
the selected timezone, while Cost Explorer shows the entity in its broader
tenant context. Dashboard and Topic Attribution Compare summarize the two date
ranges. The screenshots use a fixed example period, so your dates and totals
may differ.

FOCUS Mapping Preview is available for the Confluent Cloud tenant. It is a
preview with [known conformance limitations](../focus-mapping-preview.md), not a
conforming FOCUS export.

## Stop or start fresh

Run these commands from the repository root:

| Command | What happens |
|---|---|
| `./demo down` | Stops the demo and removes its containers. Your demo data is preserved. |
| `./demo --showcase` | Starts Showcase again, keeping existing data and tag changes. |
| `./demo` | Starts Clean again, keeping existing data and tag changes. |
| `./demo reset --showcase` | Deletes Showcase data and starts it again with fresh synthetic data. |
| `./demo reset --clean` | Deletes Clean data and starts it again with fresh synthetic data. |

!!! warning "Reset deletes your changes"
    Reset removes data and tag edits in the selected profile. The other profile's
    data is preserved. Reset also stops the running demo before restarting it.

Without a profile argument, `./demo reset` resets the last successfully started
profile, or Clean if none has been started. Reset accepts only the profile
argument; to use optional startup settings again, run the corresponding start
command after reset completes.

The demo keeps its data between runs; it does not advance the dates automatically.
If it recommends a reset because the data is old or incompatible, use the command
shown in the message. You can also select an older date range to explore existing
data when no reset is required.

## Optional settings

### Add Grafana

```bash
./demo --showcase --grafana
```

Open <http://127.0.0.1:3000> and sign in with `admin` / `password`. Set
`GF_ADMIN_PASSWORD` before starting to choose a different password. Grafana
provides dashboards over the Confluent Cloud demo data and reads that data
without modifying it.

### Change ports

```bash
./demo --showcase --ui-port 9081 --api-port 9080
```

Use the URLs printed at startup. With Grafana enabled, `--grafana-port` changes
its default port of `3000`. Choose a different port for each service, between
`1` and `65535`.

### Share a demo on your local network

```bash
./demo --showcase --lan
```

The launcher prints URLs other devices on your network can use.

!!! warning "Use a trusted network"
    LAN mode exposes the demo UI and API without authentication. Other people
    on that network can view and change the synthetic data. Use the default
    local-only mode when sharing is unnecessary.

### Run the code from your checkout

Normal startup downloads released images: the matching version for a release-tag
checkout, or `latest` otherwise. To run the code from your current checkout:

```bash
./demo --showcase --build
```

## Troubleshooting

| Problem | What to do |
|---|---|
| Docker or Compose is missing | Install Docker with Compose support, start Docker, and rerun the command. |
| A port is already in use | Stop the container or process named in the error, or choose another port with `--ui-port`, `--api-port`, or `--grafana-port`. |
| The launcher cannot inspect a port | Check the reported inspection error and your permissions to inspect local processes. The demo will not start until it can check the selected ports. |
| LAN startup cannot find an address | Check your network connection, or omit `--lan` to run locally. |
| Charts are empty | Check the selected tenant and date range. Existing demo data keeps the dates from when it was generated. |
| Startup reports incompatible data | Run the profile-specific reset command printed in the error. This deletes changes in that profile. |

If startup fails, the demo leaves containers available for diagnosis. Check their
status and logs before stopping them:

```bash
./demo status
./demo logs
./demo down
```

`logs` prints the current output once. Fix the reported problem, then run your
start command again.

Ready to use your own data? Continue with the [Quickstart](quickstart.md).
