import { Component, useState, type ReactNode } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
import { http, HttpResponse } from "msw";
import { afterEach, describe, expect, it, vi } from "vitest";
import { server } from "../../../test/mocks/server";
import {
  ResourceLinkProvider,
  useResourceLinks,
} from "../../../providers/ResourceLinkContext";
import type { ChargebackFilters } from "../../../types/filters";
import { AllocationIssuesTable } from "../AllocationIssuesTable";

ModuleRegistry.registerModules([AllCommunityModule]);

vi.mock("../../../providers/TenantContext", () => ({
  useTenant: () => ({ currentTenant: { tenant_name: "acme", ecosystem: "confluent_cloud" } }),
}));

const filters: ChargebackFilters = {
  start_date: null,
  end_date: null,
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
  timezone: null,
  tag_key: null,
  tag_value: null,
};

class CrashBoundary extends Component<{ children: ReactNode }, { error: boolean }> {
  state = { error: false };

  static getDerivedStateFromError() {
    return { error: true };
  }

  render() {
    return this.state.error ? <div>Dashboard crashed</div> : this.props.children;
  }
}

function DashboardNavigation() {
  const [open, setOpen] = useState(false);
  const { enabled, setEnabled } = useResourceLinks();
  return (
    <>
      <button onClick={() => setEnabled(!enabled)}>Links</button>
      <button onClick={() => setOpen(true)}>Dashboard</button>
      {open && (
        <>
          <h1>Cost Dashboard</h1>
          <AllocationIssuesTable tenantName="acme" filters={filters} />
        </>
      )}
    </>
  );
}

afterEach(() => localStorage.clear());

describe("AllocationIssuesTable with real grid and resource links", () => {
  it.each(["before navigation", "while loading"])("keeps the dashboard mounted when links are enabled %s", async (timing) => {
    let releaseRows!: () => void;
    const rowsReady = new Promise<void>((resolve) => {
      releaseRows = resolve;
    });
    const requestedIdentifiers: string[][] = [];
    const rowsRequested = vi.fn();
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", async () => {
        rowsRequested();
        await rowsReady;
        return HttpResponse.json({
          items: [{
            ecosystem: "confluent_cloud",
            resource_id: "lkc-demo",
            identity_id: "sa-demo",
            product_type: "KAFKA",
            allocation_detail: "NO_USAGE",
            usage_cost: "0",
            shared_cost: "10",
            total_cost: "10",
          }],
          total: 1, page: 1, page_size: 100, pages: 1,
        });
      }),
      http.post("/api/v1/tenants/acme/resource-links/resolve", async ({ request }) => {
        const { identifiers } = await request.json() as { identifiers: string[] };
        requestedIdentifiers.push(identifiers);
        return HttpResponse.json({
          resources: {
            "lkc-demo": {
              resource_type: "kafka_cluster", parent_id: "env-demo", kafka_cluster_id: null,
            },
          },
          identities: { "sa-demo": { identity_type: "service_account" } },
        });
      }),
    );
    const user = userEvent.setup();
    const { unmount } = render(
      <CrashBoundary>
        <ResourceLinkProvider><DashboardNavigation /></ResourceLinkProvider>
      </CrashBoundary>,
    );
    try {
      if (timing === "before navigation") {
        await user.click(screen.getByRole("button", { name: "Links" }));
      }
      await user.click(screen.getByRole("button", { name: "Dashboard" }));
      await waitFor(() => expect(rowsRequested).toHaveBeenCalled());
      if (timing === "while loading") {
        await user.click(screen.getByRole("button", { name: "Links" }));
      }
      expect(screen.queryByText("Dashboard crashed")).not.toBeInTheDocument();
      expect(screen.getByRole("heading", { name: "Cost Dashboard" })).toBeInTheDocument();
      expect(screen.getAllByText("—").length).toBeGreaterThan(0);
      expect(requestedIdentifiers).toEqual([]);

      await act(async () => releaseRows());
      expect(await screen.findByRole("link", { name: "lkc-demo" })).toHaveAttribute(
        "href", "https://confluent.cloud/environments/env-demo/clusters/lkc-demo",
      );
      expect(await screen.findByRole("link", { name: "sa-demo" })).toHaveAttribute(
        "href", "https://confluent.cloud/settings/principals/sa-demo?view=identity",
      );
      expect(requestedIdentifiers.flat().sort()).toEqual(["lkc-demo", "sa-demo"]);
      expect(screen.getByRole("heading", { name: "Cost Dashboard" })).toBeInTheDocument();
    } finally {
      releaseRows();
      unmount();
    }
  });
});
