import type React from "react";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TenantProvider } from "../../providers/TenantContext";
import { ResourceLinkProvider } from "../../providers/ResourceLinkContext";
import { ConfluentLinkRenderer } from "./ConfluentLinkRenderer";

// ---------------------------------------------------------------------------
// Integration test — real ResourceLinkProvider (no mocks)
// Verifies: Provider → fetch → index → resolveUrl → renderer → link
// ---------------------------------------------------------------------------

function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <TenantProvider>
      <ResourceLinkProvider>{children}</ResourceLinkProvider>
    </TenantProvider>
  );
}

beforeEach(() => {
  localStorage.setItem("chargeback_deep_links_enabled", "true");
  server.use(
    http.post("/api/v1/tenants/acme/resource-links/resolve", async ({ request }) => {
      expect(await request.json()).toEqual({ identifiers: ["lkc-def456"] });
      return HttpResponse.json({
        resources: {
          "lkc-def456": {
            resource_type: "kafka_cluster",
            parent_id: "env-abc123",
            kafka_cluster_id: null,
          },
        },
        identities: {},
      });
    }),
    http.get("/api/v1/tenants/:tenant/resources", () => {
      throw new Error("link rendering must not fetch paginated resources");
    }),
    http.get("/api/v1/tenants/:tenant/identities", () => {
      throw new Error("link rendering must not fetch paginated identities");
    }),
  );
});

afterEach(() => {
  localStorage.clear();
});

describe("ConfluentLinkRenderer — integration with real ResourceLinkProvider", () => {
  it("renders link with correct href after a real provider posts visible link context", async () => {
    render(
      <Wrapper>
        <ConfluentLinkRenderer value="lkc-def456" />
      </Wrapper>,
    );

    const link = await screen.findByRole("link");

    expect(link.getAttribute("href")).toBe(
      "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456",
    );
    expect(link.textContent).toBe("lkc-def456");
  });
});
