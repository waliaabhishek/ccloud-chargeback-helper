import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ResourceResponse } from "../../types/api";
import { ResourceDetailDrawer } from "./ResourceDetailDrawer";

vi.mock("antd", () => ({
  Drawer: ({ children }: { children: ReactNode }) => <section>{children}</section>,
  Divider: () => <hr />,
  Descriptions: Object.assign(
    ({ children }: { children: ReactNode }) => <dl>{children}</dl>,
    {
      Item: ({ children }: { children: ReactNode }) => <dd>{children}</dd>,
    },
  ),
}));

vi.mock("../entities/EntityTagEditor", () => ({
  EntityTagEditor: () => <div data-testid="tags" />,
}));

vi.mock("../common/ConfluentLinkRenderer", () => ({
  ConfluentLinkRenderer: ({ value }: { value: string | null }) => (
    <span data-testid="link-renderer">{value}</span>
  ),
}));

const resource: ResourceResponse = {
  ecosystem: "ccloud",
  tenant_id: "t-001",
  resource_id: "lkc-drawer",
  resource_type: "kafka_cluster",
  display_name: null,
  parent_id: "env-drawer",
  owner_id: null,
  status: "active",
  created_at: null,
  deleted_at: null,
  last_seen_at: null,
  metadata: {},
};

describe("ResourceDetailDrawer", () => {
  it("passes the resource identifier through the shared renderer", () => {
    render(<ResourceDetailDrawer resource={resource} tenantName="acme" onClose={vi.fn()} />);

    expect(screen.getByTestId("link-renderer")).toHaveTextContent("lkc-drawer");
  });
});
