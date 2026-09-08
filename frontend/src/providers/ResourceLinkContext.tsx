import type React from "react";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { API_URL } from "../config";
import { useTenant } from "./TenantContext";
import {
  environmentUrl,
  clusterUrl,
  schemaRegistryUrl,
  serviceAccountUrl,
  userUrl,
  identityProviderUrl,
  apiKeyUrl,
  flinkComputePoolUrl,
  ksqldbClusterUrl,
} from "../config/confluentCloudUrls";

const STORAGE_KEY = "chargeback_deep_links_enabled";
const MAX_LINK_CONTEXT_IDS = 100;

type ResourceEntry = {
  resource_type: string;
  parent_id: string | null;
  kafka_cluster_id: string | null;
};

type IdentityEntry = { identity_type: string };

type BatchResponse = {
  resources: Record<string, ResourceEntry>;
  identities: Record<string, IdentityEntry>;
};

type LinkResolutionRuntime = {
  scopeKey: string;
  tenantName: string | null;
  generation: number;
  disposed: boolean;
  visibleCounts: Map<string, number>;
  pending: Set<string>;
  inFlight: Set<string>;
  failed: Set<string>;
  controller: AbortController;
  flushTimer: ReturnType<typeof setTimeout> | null;
  draining: boolean;
};

interface ResourceLinkContextValue {
  resolveUrl: (resourceId: string) => string | null;
  registerIdentifier: (identifier: string) => () => void;
  available: boolean;
  enabled: boolean;
  setEnabled: (enabled: boolean) => void;
  isLoading: boolean;
}

const ResourceLinkContext = createContext<ResourceLinkContextValue | null>(
  null,
);

function getInitialEnabled(): boolean {
  return localStorage.getItem(STORAGE_KEY) === "true";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function emptyIndex<T>(): Record<string, T> {
  return Object.create(null) as Record<string, T>;
}

function parseBatchResponse(value: unknown): BatchResponse | null {
  if (!isRecord(value) || !isRecord(value.resources) || !isRecord(value.identities)) {
    return null;
  }

  const resources = emptyIndex<ResourceEntry>();
  for (const [identifier, entry] of Object.entries(value.resources)) {
    if (!isRecord(entry)) return null;
    const { resource_type: resourceType, parent_id: parentId, kafka_cluster_id: kafkaClusterId } = entry;
    if (
      typeof resourceType !== "string" ||
      (parentId !== null && typeof parentId !== "string") ||
      (kafkaClusterId !== null && typeof kafkaClusterId !== "string")
    ) {
      return null;
    }
    resources[identifier] = {
      resource_type: resourceType,
      parent_id: parentId,
      kafka_cluster_id: kafkaClusterId,
    };
  }

  const identities = emptyIndex<IdentityEntry>();
  for (const [identifier, entry] of Object.entries(value.identities)) {
    if (!isRecord(entry) || typeof entry.identity_type !== "string") return null;
    identities[identifier] = { identity_type: entry.identity_type };
  }

  return { resources, identities };
}

function resolveFromEntry(
  resourceId: string,
  entry: ResourceEntry,
): string | null {
  switch (entry.resource_type) {
    case "environment":
      return environmentUrl(resourceId);
    case "kafka_cluster": {
      if (!entry.parent_id) return null;
      return clusterUrl(entry.parent_id, resourceId);
    }
    case "schema_registry": {
      if (!entry.parent_id) return null;
      return schemaRegistryUrl(entry.parent_id);
    }
    case "service_account":
      return serviceAccountUrl(resourceId);
    case "flink_compute_pool": {
      if (!entry.parent_id) return null;
      return flinkComputePoolUrl(entry.parent_id, resourceId);
    }
    case "ksqldb_cluster": {
      if (!entry.parent_id || !entry.kafka_cluster_id) return null;
      return ksqldbClusterUrl(entry.parent_id, entry.kafka_cluster_id, resourceId);
    }
    default:
      return null;
  }
}

function resolveFromIdentity(
  identityId: string,
  entry: IdentityEntry,
): string | null {
  switch (entry.identity_type) {
    case "service_account":
      return serviceAccountUrl(identityId);
    case "user":
      return userUrl(identityId);
    case "identity_provider":
      return identityProviderUrl(identityId);
    case "api_key":
      return apiKeyUrl(identityId);
    default:
      return null;
  }
}

interface ResourceLinkProviderProps {
  children: ReactNode;
}

export function ResourceLinkProvider({
  children,
}: ResourceLinkProviderProps): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [enabledPreference, setEnabledState] = useState<boolean>(getInitialEnabled);
  const [revision, setRevision] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const cacheByTenantRef = useRef<Map<string, Map<string, string | null>>>(
    new Map(),
  );
  const runtimeRef = useRef<LinkResolutionRuntime | null>(null);
  const generationRef = useRef(0);
  const tenantName = currentTenant?.tenant_name ?? null;
  const available = currentTenant?.ecosystem === "confluent_cloud";
  const enabled = available && enabledPreference;
  const currentScopeKey = JSON.stringify([enabled, tenantName]);

  const publishLoading = useCallback(
    (runtime: LinkResolutionRuntime, loading: boolean): void => {
      if (
        runtimeRef.current === runtime &&
        !runtime.disposed &&
        !runtime.controller.signal.aborted
      ) {
        setIsLoading(loading);
      }
    },
    [],
  );

  const disposeRuntime = useCallback((runtime: LinkResolutionRuntime): void => {
    runtime.disposed = true;
    runtime.controller.abort();
    if (runtime.flushTimer !== null) {
      clearTimeout(runtime.flushTimer);
      runtime.flushTimer = null;
    }
  }, []);

  const ensureRuntime = useCallback(
    (scopeKey: string, nextTenantName: string | null): LinkResolutionRuntime => {
      const current = runtimeRef.current;
      if (
        current &&
        current.scopeKey === scopeKey &&
        !current.disposed &&
        !current.controller.signal.aborted
      ) {
        return current;
      }

      if (current) disposeRuntime(current);

      const runtime: LinkResolutionRuntime = {
        scopeKey,
        tenantName: nextTenantName,
        generation: generationRef.current + 1,
        disposed: false,
        visibleCounts: new Map(),
        pending: new Set(),
        inFlight: new Set(),
        failed: new Set(),
        controller: new AbortController(),
        flushTimer: null,
        draining: false,
      };
      generationRef.current = runtime.generation;
      runtimeRef.current = runtime;
      setIsLoading(false);
      return runtime;
    },
    [disposeRuntime],
  );

  const drainRuntime = useCallback(
    async (runtime: LinkResolutionRuntime): Promise<void> => {
      if (
        runtime.disposed ||
        runtime.controller.signal.aborted ||
        runtime.draining ||
        runtime.tenantName === null
      ) {
        return;
      }

      runtime.draining = true;
      publishLoading(runtime, true);
      try {
        while (
          runtimeRef.current === runtime &&
          !runtime.disposed &&
          !runtime.controller.signal.aborted
        ) {
          const cache = cacheByTenantRef.current.get(runtime.tenantName);
          const eligible: string[] = [];
          for (const identifier of runtime.pending) {
            if ((runtime.visibleCounts.get(identifier) ?? 0) <= 0) {
              runtime.pending.delete(identifier);
              continue;
            }
            if (
              cache?.has(identifier) ||
              runtime.inFlight.has(identifier) ||
              runtime.failed.has(identifier)
            ) {
              runtime.pending.delete(identifier);
              continue;
            }
            eligible.push(identifier);
            if (eligible.length === MAX_LINK_CONTEXT_IDS) break;
          }

          if (eligible.length === 0) break;

          for (const identifier of eligible) {
            runtime.pending.delete(identifier);
            runtime.inFlight.add(identifier);
          }

          try {
            const response = await fetch(
              `${API_URL}/tenants/${encodeURIComponent(runtime.tenantName)}/resource-links/resolve`,
              {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ identifiers: eligible }),
                signal: runtime.controller.signal,
              },
            );
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const payload = parseBatchResponse(await response.json());
            if (payload === null) throw new Error("Invalid resource link response");

            if (
              runtimeRef.current !== runtime ||
              runtime.disposed ||
              runtime.controller.signal.aborted
            ) {
              return;
            }

            const tenantCache =
              cacheByTenantRef.current.get(runtime.tenantName) ??
              new Map<string, string | null>();
            for (const identifier of eligible) {
              const resource = Object.prototype.hasOwnProperty.call(
                payload.resources,
                identifier,
              )
                ? payload.resources[identifier]
                : undefined;
              const identity = Object.prototype.hasOwnProperty.call(
                payload.identities,
                identifier,
              )
                ? payload.identities[identifier]
                : undefined;
              const url = resource
                ? resolveFromEntry(identifier, resource)
                : identity
                  ? resolveFromIdentity(identifier, identity)
                  : null;
              tenantCache.set(identifier, url);
              runtime.inFlight.delete(identifier);
            }
            cacheByTenantRef.current.set(runtime.tenantName, tenantCache);
            setRevision((value) => value + 1);
          } catch (error) {
            for (const identifier of eligible) runtime.inFlight.delete(identifier);
            if (
              runtime.controller.signal.aborted ||
              (error instanceof Error && error.name === "AbortError")
            ) {
              return;
            }
            if (runtimeRef.current !== runtime || runtime.disposed) return;
            for (const identifier of eligible) runtime.failed.add(identifier);
          }
        }
      } finally {
        runtime.draining = false;
        if (
          runtimeRef.current === runtime &&
          !runtime.disposed &&
          !runtime.controller.signal.aborted
        ) {
          const cache = runtime.tenantName
            ? cacheByTenantRef.current.get(runtime.tenantName)
            : undefined;
          publishLoading(runtime, false);
          for (const identifier of runtime.pending) {
            if (
              (runtime.visibleCounts.get(identifier) ?? 0) <= 0 ||
              cache?.has(identifier) ||
              runtime.failed.has(identifier)
            ) {
              runtime.pending.delete(identifier);
            }
          }
        }
      }
    },
    [publishLoading],
  );

  const scheduleFlush = useCallback(
    (runtime: LinkResolutionRuntime): void => {
      if (
        runtime.flushTimer !== null ||
        runtime.disposed ||
        runtime.controller.signal.aborted
      ) {
        return;
      }
      runtime.flushTimer = setTimeout(() => {
        runtime.flushTimer = null;
        void drainRuntime(runtime);
      }, 0);
    },
    [drainRuntime],
  );

  const registerIdentifier = useCallback(
    (identifier: string): (() => void) => {
      if (!enabled || tenantName === null || !identifier.trim()) {
        return () => undefined;
      }

      const runtime = ensureRuntime(currentScopeKey, tenantName);
      const generation = runtime.generation;
      runtime.visibleCounts.set(
        identifier,
        (runtime.visibleCounts.get(identifier) ?? 0) + 1,
      );
      const cache = cacheByTenantRef.current.get(tenantName);
      if (
        !cache?.has(identifier) &&
        !runtime.pending.has(identifier) &&
        !runtime.inFlight.has(identifier) &&
        !runtime.failed.has(identifier)
      ) {
        runtime.pending.add(identifier);
        scheduleFlush(runtime);
      }

      return () => {
        const current = runtimeRef.current;
        if (current === null || current.generation !== generation) return;
        const count = current.visibleCounts.get(identifier) ?? 0;
        if (count <= 1) {
          current.visibleCounts.delete(identifier);
          if (!current.inFlight.has(identifier)) current.pending.delete(identifier);
        } else {
          current.visibleCounts.set(identifier, count - 1);
        }
      };
    },
    [currentScopeKey, enabled, ensureRuntime, scheduleFlush, tenantName],
  );

  const setEnabled = useCallback((value: boolean): void => {
    if (!available) return;
    localStorage.setItem(STORAGE_KEY, String(value));
    setEnabledState(value);
  }, [available]);

  useLayoutEffect(() => {
    ensureRuntime(currentScopeKey, tenantName);
  }, [currentScopeKey, ensureRuntime, tenantName]);

  useEffect(() => {
    return () => {
      const runtime = runtimeRef.current;
      if (runtime) disposeRuntime(runtime);
    };
  }, [disposeRuntime]);

  const resolveUrl = useCallback(
    (resourceId: string): string | null => {
      if (!enabled || tenantName === null) return null;
      const cache = cacheByTenantRef.current.get(tenantName);
      if (!cache?.has(resourceId)) return null;
      return cache.get(resourceId) ?? null;
    },
    [enabled, tenantName],
  );

  const value = useMemo<ResourceLinkContextValue>(
    () => {
      void revision;
      return {
        resolveUrl,
        registerIdentifier,
        available,
        enabled,
        setEnabled,
        isLoading,
      };
    },
    [resolveUrl, registerIdentifier, available, enabled, setEnabled, isLoading, revision],
  );

  return (
    <ResourceLinkContext.Provider value={value}>
      {children}
    </ResourceLinkContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useResourceLinks(): ResourceLinkContextValue {
  const ctx = useContext(ResourceLinkContext);
  if (!ctx) {
    throw new Error(
      "useResourceLinks must be used within ResourceLinkProvider",
    );
  }
  return ctx;
}
