import type React from "react";
import { StrictMode, useEffect, useLayoutEffect } from "react";
import { act, render, renderHook, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../test/mocks/server";
import { ConfluentLinkRenderer } from "../components/common/ConfluentLinkRenderer";
import { ResourceLinkProvider, useResourceLinks } from "./ResourceLinkContext";
import { TenantProvider, useTenant } from "./TenantContext";

const RESOLVE_PATH = "/api/v1/tenants/:tenant/resource-links/resolve";

type BatchResponse = {
  resources: Record<
    string,
    {
      resource_type: string;
      parent_id: string | null;
      kafka_cluster_id: string | null;
    }
  >;
  identities: Record<string, { identity_type: string }>;
};

type BatchRequest = { tenant: string; identifiers: string[] };

function response(
  resources: BatchResponse["resources"] = {},
  identities: BatchResponse["identities"] = {},
): BatchResponse {
  return { resources, identities };
}

function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <TenantProvider>
      <ResourceLinkProvider>{children}</ResourceLinkProvider>
    </TenantProvider>
  );
}

type Controller = {
  links: ReturnType<typeof useResourceLinks>;
  tenant: ReturnType<typeof useTenant>;
};

function ControllerProbe({
  onReady,
  value,
}: {
  onReady: (controller: Controller) => void;
  value?: string;
}): React.JSX.Element {
  const links = useResourceLinks();
  const tenant = useTenant();

  useEffect(() => onReady({ links, tenant }), [links, onReady, tenant]);
  return value === undefined ? (
    <span>probe</span>
  ) : (
    <ConfluentLinkRenderer value={value} />
  );
}

function LayoutRegistrationProbe({ value }: { value: string }): React.JSX.Element {
  const { registerIdentifier } = useResourceLinks();

  useLayoutEffect(() => registerIdentifier(value), [registerIdentifier, value]);
  return <span>layout-probe</span>;
}

function installBatchHandler(
  handler: (request: BatchRequest) => Response | Promise<Response>,
): BatchRequest[] {
  const calls: BatchRequest[] = [];
  server.use(
    http.post(RESOLVE_PATH, async ({ params, request }) => {
      const body = (await request.json()) as { identifiers: string[] };
      const batchRequest = {
        tenant: String(params.tenant),
        identifiers: body.identifiers,
      };
      calls.push(batchRequest);
      return handler(batchRequest);
    }),
    http.get("/api/v1/tenants/:tenant/resources", () => {
      throw new Error("link resolution must not load paginated resources");
    }),
    http.get("/api/v1/tenants/:tenant/identities", () => {
      throw new Error("link resolution must not load paginated identities");
    }),
  );
  return calls;
}

async function waitForCalls(calls: BatchRequest[], count: number): Promise<void> {
  await waitFor(() => expect(calls).toHaveLength(count));
}

afterEach(() => {
  localStorage.clear();
});

describe("ResourceLinkProvider batch registration", () => {
  it("defaults off and makes no link-context request until registration is enabled", async () => {
    const calls = installBatchHandler(() => HttpResponse.json(response()));

    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    act(() => {
      result.current.links.registerIdentifier("env-hidden");
    });

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    expect(result.current.links.enabled).toBe(false);
    expect(calls).toHaveLength(0);
  });

  it("does not request identifiers registered with no selected tenant", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() => HttpResponse.json(response()));
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());
    act(() => result.current.tenant.setCurrentTenant(null));
    await waitFor(() => expect(result.current.tenant.currentTenant).toBeNull());
    act(() => result.current.links.registerIdentifier("env-without-tenant"));
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });

    expect(calls).toHaveLength(0);
  });

  it("does not retain a no-tenant registration when a tenant is selected later", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() => HttpResponse.json(response()));
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());
    const tenant = result.current.tenant.currentTenant!;
    act(() => result.current.tenant.setCurrentTenant(null));
    await waitFor(() => expect(result.current.tenant.currentTenant).toBeNull());
    act(() => result.current.links.registerIdentifier("env-not-retained"));
    act(() => result.current.tenant.setCurrentTenant(tenant));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("acme"));
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });

    expect(calls).toHaveLength(0);
  });

  it("does not schedule empty or whitespace-only identifiers and preserves nonblank bytes", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() => HttpResponse.json(response()));
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => {
      result.current.links.registerIdentifier("");
      result.current.links.registerIdentifier("   ");
      result.current.links.registerIdentifier("  u-kept  ");
    });

    await waitForCalls(calls, 1);
    expect(calls[0]).toEqual({ tenant: "acme", identifiers: ["  u-kept  "] });
  });

  it("coalesces duplicate mounted identifiers and removes an unused identifier before dispatch", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    vi.useFakeTimers();
    const calls = installBatchHandler(() => HttpResponse.json(response()));
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    let cleanupFirst: () => void = () => undefined;
    let cleanupSecond: () => void = () => undefined;
    let cleanupLastFirst: () => void = () => undefined;
    let cleanupLastSecond: () => void = () => undefined;
    let removeUnused: () => void = () => undefined;
    act(() => {
      cleanupFirst = result.current.links.registerIdentifier("env-duplicate");
      cleanupSecond = result.current.links.registerIdentifier("env-duplicate");
      cleanupLastFirst = result.current.links.registerIdentifier("env-last-cleanup");
      cleanupLastSecond = result.current.links.registerIdentifier("env-last-cleanup");
      removeUnused = result.current.links.registerIdentifier("env-unmounted");
      cleanupFirst();
      cleanupLastFirst();
      cleanupLastSecond();
      removeUnused();
      vi.advanceTimersByTime(0);
    });

    await waitForCalls(calls, 1);
    expect(calls[0].identifiers).toEqual(["env-duplicate"]);
    act(() => cleanupSecond());
  });

  it.each([
    [1, 1],
    [100, 1],
    [101, 2],
    [251, 3],
  ])(
    "sends %i visible identifiers in %i bounded sequential batches independent of catalog size",
    async (visibleCount, expectedCalls) => {
      localStorage.setItem("chargeback_deep_links_enabled", "true");
      const catalog = new Set(
        Array.from({ length: 10_000 }, (_, index) => `environment-${index}`),
      );
      const calls = installBatchHandler(({ identifiers }) => {
        const resources = Object.fromEntries(
          identifiers.filter((identifier) => catalog.has(identifier)).map((identifier) => [
            identifier,
            {
              resource_type: "environment",
              parent_id: null,
              kafka_cluster_id: null,
            },
          ]),
        );
        return HttpResponse.json(response(resources));
      });
      const { result } = renderHook(
        () => ({ links: useResourceLinks(), tenant: useTenant() }),
        { wrapper: Wrapper },
      );
      await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

      act(() => {
        for (let index = 0; index < visibleCount; index += 1) {
          result.current.links.registerIdentifier(`environment-${index}`);
        }
      });

      await waitForCalls(calls, expectedCalls);
      expect(calls.flatMap((call) => call.identifiers)).toHaveLength(visibleCount);
      expect(calls.every((call) => call.identifiers.length <= 100)).toBe(true);
      const requestedIdentifiers = Array.from(
        { length: visibleCount },
        (_, index) => `environment-${index}`,
      );
      expect(calls.flatMap((call) => call.identifiers)).toEqual(requestedIdentifiers);
      await waitFor(() => {
        expect(
          requestedIdentifiers.every(
            (identifier) =>
              result.current.links.resolveUrl(identifier) ===
              `https://confluent.cloud/environments/${identifier}`,
          ),
        ).toBe(true);
      });
      expect(
        requestedIdentifiers.filter(
          (identifier) => result.current.links.resolveUrl(identifier) !== null,
        ),
      ).toHaveLength(visibleCount);
    },
  );

  it("caches successful URLs and definitive misses across unmount and remount", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(({ identifiers }) =>
      HttpResponse.json(
        response({
          [identifiers[0]]: {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      ),
    );
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    let cleanupKnown: () => void = () => undefined;
    let cleanupMissing: () => void = () => undefined;
    act(() => {
      cleanupKnown = result.current.links.registerIdentifier("env-cached");
      cleanupMissing = result.current.links.registerIdentifier("missing-cached");
    });
    await waitForCalls(calls, 1);
    await waitFor(() => {
      expect(result.current.links.resolveUrl("env-cached")).toBe(
        "https://confluent.cloud/environments/env-cached",
      );
      expect(result.current.links.resolveUrl("missing-cached")).toBeNull();
    });

    act(() => {
      cleanupKnown();
      cleanupMissing();
      result.current.links.registerIdentifier("env-cached");
      result.current.links.registerIdentifier("missing-cached");
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });

    expect(calls).toHaveLength(1);

    const tenantA = result.current.tenant.currentTenant!;
    const tenantB = result.current.tenant.tenants.find(
      (tenant) => tenant.tenant_name === "globex",
    )!;
    act(() => result.current.tenant.setCurrentTenant(tenantB));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("globex"));
    act(() => result.current.tenant.setCurrentTenant(tenantA));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("acme"));
    act(() => {
      result.current.links.registerIdentifier("env-cached");
      result.current.links.registerIdentifier("missing-cached");
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    expect(calls).toHaveLength(1);
  });

  it("keeps resource precedence when an unsupported resource collides with a supported identity", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(({ identifiers }) =>
      HttpResponse.json(
        response(
          {
            [identifiers[0]]: {
              resource_type: "connector",
              parent_id: "lkc-parent",
              kafka_cluster_id: null,
            },
          },
          { [identifiers[0]]: { identity_type: "user" } },
        ),
      ),
    );
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => {
      result.current.links.registerIdentifier("collision");
    });
    await waitForCalls(calls, 1);
    await waitFor(() => expect(result.current.links.resolveUrl("collision")).toBeNull());
  });

  it("resolves prototype-sensitive identifiers with resource precedence and safe misses", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const resourceEntry = (resourceType: string) => ({
      resource_type: resourceType,
      parent_id: null,
      kafka_cluster_id: null,
    });
    const resources = Object.fromEntries(
      ["constructor", "toString", "__proto__"].map((identifier) => [
        identifier,
        resourceEntry("environment"),
      ]),
    );
    const identities = Object.fromEntries(
      ["constructor", "toString", "__proto__", "identity-only"].map((identifier) => [
        identifier,
        { identity_type: "user" },
      ]),
    );
    const calls = installBatchHandler(() =>
      HttpResponse.json(response(resources, identities)),
    );
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    const identifiers = ["constructor", "toString", "__proto__", "identity-only", "unknown"];
    act(() => {
      for (const identifier of identifiers) result.current.links.registerIdentifier(identifier);
    });
    await waitForCalls(calls, 1);
    await waitFor(() => {
      expect(result.current.links.resolveUrl("constructor")).toBe(
        "https://confluent.cloud/environments/constructor",
      );
      expect(result.current.links.resolveUrl("toString")).toBe(
        "https://confluent.cloud/environments/toString",
      );
      expect(result.current.links.resolveUrl("__proto__")).toBe(
        "https://confluent.cloud/environments/__proto__",
      );
      expect(result.current.links.resolveUrl("identity-only")).toBe(
        "https://confluent.cloud/settings/principals/identity-only?view=identity",
      );
      expect(result.current.links.resolveUrl("unknown")).toBeNull();
    });
  });

  it("resolves every supported resource and identity URL from minimal batch entries", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const resources = {
      "env-1": { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
      "lkc-1": { resource_type: "kafka_cluster", parent_id: "env-1", kafka_cluster_id: null },
      "lsrc-1": { resource_type: "schema_registry", parent_id: "env-1", kafka_cluster_id: null },
      "resource-sa": { resource_type: "service_account", parent_id: null, kafka_cluster_id: null },
      "lfcp-1": { resource_type: "flink_compute_pool", parent_id: "env-1", kafka_cluster_id: null },
      "lksqlc-1": { resource_type: "ksqldb_cluster", parent_id: "env-1", kafka_cluster_id: "lkc-1" },
    };
    const identities = {
      "identity-sa": { identity_type: "service_account" },
      "user-1": { identity_type: "user" },
      "op-1": { identity_type: "identity_provider" },
      "api-key-1": { identity_type: "api_key" },
    };
    const calls = installBatchHandler(() => HttpResponse.json(response(resources, identities)));
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => {
      for (const identifier of [...Object.keys(resources), ...Object.keys(identities)]) {
        result.current.links.registerIdentifier(identifier);
      }
    });
    await waitForCalls(calls, 1);
    await waitFor(() => {
      expect(result.current.links.resolveUrl("lksqlc-1")).toBe(
        "https://confluent.cloud/environments/env-1/clusters/lkc-1/ksql/lksqlc-1/editor",
      );
    });

    expect(result.current.links.resolveUrl("env-1")).toBe("https://confluent.cloud/environments/env-1");
    expect(result.current.links.resolveUrl("lkc-1")).toBe("https://confluent.cloud/environments/env-1/clusters/lkc-1");
    expect(result.current.links.resolveUrl("lsrc-1")).toBe(
      "https://confluent.cloud/environments/env-1/stream-governance/schema-registry/overview",
    );
    expect(result.current.links.resolveUrl("resource-sa")).toBe(
      "https://confluent.cloud/settings/principals/resource-sa?view=identity",
    );
    expect(result.current.links.resolveUrl("lfcp-1")).toBe(
      "https://confluent.cloud/environments/env-1/flink/pools/lfcp-1/overview",
    );
    expect(result.current.links.resolveUrl("identity-sa")).toBe(
      "https://confluent.cloud/settings/principals/identity-sa?view=identity",
    );
    expect(result.current.links.resolveUrl("user-1")).toBe(
      "https://confluent.cloud/settings/principals/user-1?view=identity",
    );
    expect(result.current.links.resolveUrl("op-1")).toBe(
      "https://confluent.cloud/settings/org/workload_identities/provider/oidc/view/op-1",
    );
    expect(result.current.links.resolveUrl("api-key-1")).toBe(
      "https://confluent.cloud/settings/api-keys/edit/api-key-1",
    );
  });

  it("keeps deleted, unknown, unsupported, connector, identity-pool, and incomplete parent entries plain text", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() =>
      HttpResponse.json(
        response(
          {
            unsupported: { resource_type: "unknown", parent_id: null, kafka_cluster_id: null },
            connector: { resource_type: "connector", parent_id: "lkc-1", kafka_cluster_id: null },
            orphanCluster: { resource_type: "kafka_cluster", parent_id: null, kafka_cluster_id: null },
            orphanKsql: { resource_type: "ksqldb_cluster", parent_id: "env-1", kafka_cluster_id: null },
          },
          { pool: { identity_type: "identity_pool" } },
        ),
      ),
    );
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());
    const identifiers = [
      "deleted-omitted",
      "unknown-omitted",
      "unsupported",
      "connector",
      "pool",
      "orphanCluster",
      "orphanKsql",
      "sa-not-authoritative",
      "env-not-authoritative",
      "u-not-authoritative",
      "op-not-authoritative",
    ];

    act(() => {
      for (const identifier of identifiers) result.current.links.registerIdentifier(identifier);
    });
    await waitForCalls(calls, 1);
    await waitFor(() => expect(result.current.links.resolveUrl("connector")).toBeNull());

    for (const identifier of identifiers) {
      expect(result.current.links.resolveUrl(identifier)).toBeNull();
    }
  });
});

describe("ResourceLinkProvider generations and failures", () => {
  it("immediately clears loading when disabling or clearing the tenant while a request is in flight", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const deferred: Array<(response: Response) => void> = [];
    const calls = installBatchHandler(
      () =>
        new Promise<Response>((resolve) => deferred.push(resolve)),
    );
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => result.current.links.registerIdentifier("env-loading"));
    await waitForCalls(calls, 1);
    await waitFor(() => expect(result.current.links.isLoading).toBe(true));
    act(() => result.current.links.setEnabled(false));
    expect(result.current.links.isLoading).toBe(false);
    act(() => result.current.links.setEnabled(true));
    act(() => result.current.links.registerIdentifier("env-loading-again"));
    await waitForCalls(calls, 2);
    await waitFor(() => expect(result.current.links.isLoading).toBe(true));
    const tenantA = result.current.tenant.currentTenant!;
    act(() => result.current.tenant.setCurrentTenant(null));
    expect(result.current.links.isLoading).toBe(false);

    await act(async () => {
      deferred[0](HttpResponse.json(response()));
      deferred[1](
        HttpResponse.json(
          response({
            "env-loading-again": {
              resource_type: "environment",
              parent_id: null,
              kafka_cluster_id: null,
            },
          }),
        ),
      );
      await Promise.resolve();
    });
    expect(result.current.links.isLoading).toBe(false);
    expect(result.current.links.resolveUrl("env-loading-again")).toBeNull();

    act(() => result.current.tenant.setCurrentTenant(tenantA));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("acme"));
    act(() => result.current.links.registerIdentifier("env-loading-again"));
    await waitForCalls(calls, 3);
    await act(async () => {
      deferred[2](
        HttpResponse.json(
          response({
            "env-loading-again": {
              resource_type: "environment",
              parent_id: null,
              kafka_cluster_id: null,
            },
          }),
        ),
      );
    });
    await waitFor(() => {
      expect(result.current.links.resolveUrl("env-loading-again")).toBe(
        "https://confluent.cloud/environments/env-loading-again",
      );
    });
  });

  it("continues with newly queued identifiers after a failed batch and suppresses failed identifiers in the same generation", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    let failFirst: ((response: Response) => void) | undefined;
    const calls = installBatchHandler(({ identifiers }) => {
      if (identifiers.includes("env-failed")) {
        return new Promise<Response>((resolve) => {
          failFirst = resolve;
        });
      }
      return HttpResponse.json(
        response({
          "env-later": {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      );
    });
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => result.current.links.registerIdentifier("env-failed"));
    await waitForCalls(calls, 1);
    act(() => result.current.links.registerIdentifier("env-later"));
    await act(async () => {
      failFirst?.(new HttpResponse(null, { status: 500 }));
    });
    await waitForCalls(calls, 2);
    expect(calls[1].identifiers).toEqual(["env-later"]);
    await waitFor(() => {
      expect(result.current.links.resolveUrl("env-later")).toBe(
        "https://confluent.cloud/environments/env-later",
      );
    });

    act(() => result.current.links.registerIdentifier("env-failed"));
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    expect(calls).toHaveLength(2);
  });

  it("retries a failed identifier only after a flag generation transition", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    let attempts = 0;
    const calls = installBatchHandler(() => {
      attempts += 1;
      return attempts === 1
        ? new HttpResponse(null, { status: 500 })
        : HttpResponse.json(
            response({
              "env-retry": {
                resource_type: "environment",
                parent_id: null,
                kafka_cluster_id: null,
              },
            }),
          );
    });
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

    act(() => result.current.links.registerIdentifier("env-retry"));
    await waitForCalls(calls, 1);
    await waitFor(() => expect(result.current.links.isLoading).toBe(false));
    act(() => result.current.links.setEnabled(false));
    await waitFor(() => expect(result.current.links.enabled).toBe(false));
    act(() => result.current.links.setEnabled(true));
    await waitFor(() => expect(result.current.links.enabled).toBe(true));
    act(() => result.current.links.registerIdentifier("env-retry"));
    await waitForCalls(calls, 2);
    await waitFor(() => {
      expect(result.current.links.resolveUrl("env-retry")).toBe(
        "https://confluent.cloud/environments/env-retry",
      );
    });
  });

  it.each(["network", "invalid-json", "malformed"] as const)(
    "suppresses a %s response failure, continues the queue, and retries after a generation transition",
    async (failure) => {
      localStorage.setItem("chargeback_deep_links_enabled", "true");
      let attempts = 0;
      const calls = installBatchHandler(({ identifiers }) => {
        attempts += 1;
        if (attempts === 1) {
          if (failure === "network") return HttpResponse.error();
          if (failure === "invalid-json") {
            return HttpResponse.text("{", {
              headers: { "Content-Type": "application/json" },
            });
          }
          return HttpResponse.json({ resources: [], identities: [] });
        }
        return HttpResponse.json(
          response(
            Object.fromEntries(
              identifiers.map((identifier) => [
                identifier,
                { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
              ]),
            ),
          ),
        );
      });
      const { result } = renderHook(
        () => ({ links: useResourceLinks(), tenant: useTenant() }),
        { wrapper: Wrapper },
      );
      await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());

      act(() => result.current.links.registerIdentifier("env-failed"));
      await waitForCalls(calls, 1);
      act(() => result.current.links.registerIdentifier("env-later"));
      await waitForCalls(calls, 2);
      expect(calls[1].identifiers).toEqual(["env-later"]);
      await waitFor(() => {
        expect(result.current.links.resolveUrl("env-later")).toBe(
          "https://confluent.cloud/environments/env-later",
        );
      });
      expect(result.current.links.resolveUrl("env-failed")).toBeNull();

      act(() => result.current.links.registerIdentifier("env-failed"));
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
      });
      expect(calls).toHaveLength(2);

      act(() => result.current.links.setEnabled(false));
      await waitFor(() => expect(result.current.links.enabled).toBe(false));
      act(() => result.current.links.setEnabled(true));
      await waitFor(() => expect(result.current.links.enabled).toBe(true));
      act(() => result.current.links.registerIdentifier("env-failed"));
      await waitForCalls(calls, 3);
      await waitFor(() => {
        expect(result.current.links.resolveUrl("env-failed")).toBe(
          "https://confluent.cloud/environments/env-failed",
        );
      });
    },
  );

  it("does not let a stale A response populate B or a later A generation", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const deferred: Array<(response: Response) => void> = [];
    const calls = installBatchHandler(
      () =>
        new Promise<Response>((resolve) => {
          deferred.push(resolve);
        }),
    );
    let controller: Controller | undefined;
    const onReady = (next: Controller): void => {
      controller = next;
    };

    render(
      <Wrapper>
        <ControllerProbe onReady={onReady} value="env-a" />
      </Wrapper>,
    );
    await waitFor(() => expect(controller?.tenant.currentTenant?.tenant_name).toBe("acme"));
    await waitForCalls(calls, 1);
    const tenantA = controller!.tenant.currentTenant!;
    const tenantB = controller!.tenant.tenants.find((tenant) => tenant.tenant_name === "globex")!;

    act(() => controller!.tenant.setCurrentTenant(tenantB));
    await waitForCalls(calls, 2);
    await act(async () => {
      deferred[0](
        HttpResponse.json(
          response({
            "env-a": { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
          }),
        ),
      );
    });
    expect(screen.queryByRole("link")).toBeNull();

    act(() => controller!.tenant.setCurrentTenant(tenantA));
    await waitForCalls(calls, 3);
    await act(async () => {
      deferred[2](
        HttpResponse.json(
          response({
            "env-a": { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
          }),
        ),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("link")).toHaveAttribute(
        "href",
        "https://confluent.cloud/environments/env-a",
      );
    });
  });

  it("keeps a new A registration when an old A cleanup runs after A to B to A", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    let releaseFirst: ((response: Response) => void) | undefined;
    let firstRequest = true;
    const calls = installBatchHandler(() => {
      if (firstRequest) {
        firstRequest = false;
        return new Promise<Response>((resolve) => {
          releaseFirst = resolve;
        });
      }
      return HttpResponse.json(response());
    });
    const { result } = renderHook(
      () => ({ links: useResourceLinks(), tenant: useTenant() }),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(result.current.tenant.currentTenant).not.toBeNull());
    const tenantA = result.current.tenant.currentTenant!;
    const tenantB = result.current.tenant.tenants.find((tenant) => tenant.tenant_name === "globex")!;
    let staleCleanup: () => void = () => undefined;

    act(() => {
      staleCleanup = result.current.links.registerIdentifier("env-generation");
    });
    await waitForCalls(calls, 1);
    act(() => result.current.tenant.setCurrentTenant(tenantB));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("globex"));
    act(() => result.current.tenant.setCurrentTenant(tenantA));
    await waitFor(() => expect(result.current.tenant.currentTenant?.tenant_name).toBe("acme"));
    act(() => {
      result.current.links.registerIdentifier("env-generation");
      staleCleanup();
    });
    releaseFirst?.(HttpResponse.json(response()));
    await waitForCalls(calls, 2);
    expect(calls[1]).toEqual({ tenant: "acme", identifiers: ["env-generation"] });
  });

  it("recreates a same-scope runtime after StrictMode effect replay and resolves the mounted renderer", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() =>
      HttpResponse.json(
        response({
          "env-strict": {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      ),
    );

    render(
      <StrictMode>
        <Wrapper>
          <ConfluentLinkRenderer value="env-strict" />
        </Wrapper>
      </StrictMode>,
    );
    await waitForCalls(calls, 1);
    expect(calls[0].identifiers).toEqual(["env-strict"]);
    await waitFor(() => {
      expect(screen.getByRole("link")).toHaveAttribute(
        "href",
        "https://confluent.cloud/environments/env-strict",
      );
    });
  });
});

describe("ResourceLinkProvider and ConfluentLinkRenderer integration", () => {
  it("re-registers a mounted renderer when a selected tenant follows a null tenant", async () => {
    const calls = installBatchHandler(() =>
      HttpResponse.json(
        response({
          "env-null-selected": {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      ),
    );
    let controller: Controller | undefined;

    render(
      <Wrapper>
        <ControllerProbe onReady={(next) => { controller = next; }} value="env-null-selected" />
      </Wrapper>,
    );
    await waitFor(() => expect(controller?.tenant.currentTenant).not.toBeNull());
    const tenant = controller!.tenant.currentTenant!;
    act(() => controller!.tenant.setCurrentTenant(null));
    await waitFor(() => expect(controller?.tenant.currentTenant).toBeNull());
    act(() => controller!.links.setEnabled(true));
    await waitFor(() => expect(controller?.links.enabled).toBe(true));
    act(() => controller!.tenant.setCurrentTenant(tenant));
    await waitForCalls(calls, 1);
    expect(calls[0]).toEqual({ tenant: "acme", identifiers: ["env-null-selected"] });
  });

  it("establishes the runtime before a child layout registration and keeps it idempotent", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() =>
      HttpResponse.json(
        response({
          "env-layout": {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      ),
    );

    render(
      <Wrapper>
        <LayoutRegistrationProbe value="env-layout" />
      </Wrapper>,
    );
    await waitForCalls(calls, 1);
    expect(calls[0]).toEqual({ tenant: "acme", identifiers: ["env-layout"] });
  });

  it("keeps a direct topic URL client-only without a batch request", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const calls = installBatchHandler(() => HttpResponse.json(response()));
    let listRequests = 0;
    server.use(
      http.get("/api/v1/tenants/:tenant/resources", () => {
        listRequests += 1;
        return HttpResponse.json({ items: [], page: 1, pages: 1 });
      }),
      http.get("/api/v1/tenants/:tenant/identities", () => {
        listRequests += 1;
        return HttpResponse.json({ items: [], page: 1, pages: 1 });
      }),
    );

    render(
      <Wrapper>
        <ConfluentLinkRenderer
          value="topic-direct"
          url="https://confluent.cloud/environments/env-direct/clusters/lkc-direct/topics/topic-direct"
        />
      </Wrapper>,
    );

    expect(screen.getByRole("link", { name: "topic-direct" })).toHaveAttribute(
      "href",
      "https://confluent.cloud/environments/env-direct/clusters/lkc-direct/topics/topic-direct",
    );
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    expect(calls).toHaveLength(0);
    expect(listRequests).toBe(0);
  });

  it("posts a mounted identifier only after enabling links and rerenders it as a link", async () => {
    const calls = installBatchHandler(() =>
      HttpResponse.json(
        response({
          "env-toggle": {
            resource_type: "environment",
            parent_id: null,
            kafka_cluster_id: null,
          },
        }),
      ),
    );
    let controller: Controller | undefined;

    render(
      <Wrapper>
        <ControllerProbe onReady={(next) => { controller = next; }} value="env-toggle" />
      </Wrapper>,
    );
    await waitFor(() => expect(controller).toBeDefined());
    expect(screen.queryByRole("link")).toBeNull();
    expect(calls).toHaveLength(0);

    act(() => controller!.links.setEnabled(true));
    await waitForCalls(calls, 1);
    expect(calls[0].identifiers).toEqual(["env-toggle"]);
    await waitFor(() => {
      expect(screen.getByRole("link")).toHaveAttribute(
        "href",
        "https://confluent.cloud/environments/env-toggle",
      );
    });
  });

  it("posts B's first mounted value once, rejects A's stale response, and reuses only settled tenant caches", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    const deferred: Array<(response: Response) => void> = [];
    const calls = installBatchHandler(
      () => new Promise<Response>((resolve) => deferred.push(resolve)),
    );
    let controller: Controller | undefined;
    const { rerender } = render(
      <Wrapper>
        <ControllerProbe onReady={(next) => { controller = next; }} value="env-a" />
      </Wrapper>,
    );
    await waitForCalls(calls, 1);
    const tenantA = controller!.tenant.currentTenant!;
    const tenantB = controller!.tenant.tenants.find((tenant) => tenant.tenant_name === "globex")!;

    act(() => controller!.tenant.setCurrentTenant(tenantB));
    rerender(
      <Wrapper>
        <ControllerProbe onReady={(next) => { controller = next; }} value="env-b" />
      </Wrapper>,
    );
    await waitForCalls(calls, 2);
    expect(calls[1]).toEqual({ tenant: "globex", identifiers: ["env-b"] });
    await act(async () => {
      deferred[0](
        HttpResponse.json(
          response({
            "env-a": { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
          }),
        ),
      );
    });
    expect(screen.queryByRole("link")).toBeNull();

    await act(async () => {
      deferred[1](
        HttpResponse.json(
          response({
            "env-b": { resource_type: "environment", parent_id: null, kafka_cluster_id: null },
          }),
        ),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("link")).toHaveAttribute(
        "href",
        "https://confluent.cloud/environments/env-b",
      );
    });

    act(() => controller!.tenant.setCurrentTenant(tenantA));
    rerender(
      <Wrapper>
        <ControllerProbe onReady={(next) => { controller = next; }} value="env-a" />
      </Wrapper>,
    );
    await waitForCalls(calls, 3);
    expect(calls[2]).toEqual({ tenant: "acme", identifiers: ["env-a"] });
  });
});

describe("ResourceLinkProvider guard", () => {
  it("throws outside its provider", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      expect(() => renderHook(() => useResourceLinks())).toThrow(/ResourceLinkProvider/);
    } finally {
      consoleError.mockRestore();
    }
  });
});
