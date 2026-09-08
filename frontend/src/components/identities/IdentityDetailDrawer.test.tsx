import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { IdentityResponse } from "../../types/api";
import { IdentityDetailDrawer } from "./IdentityDetailDrawer";

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

const identity: IdentityResponse = {
  ecosystem: "ccloud",
  tenant_id: "t-001",
  identity_id: "user-drawer",
  identity_type: "user",
  display_name: null,
  created_at: null,
  deleted_at: null,
  last_seen_at: null,
  metadata: {},
};

describe("IdentityDetailDrawer", () => {
  it("passes the identity identifier through the shared renderer", () => {
    render(<IdentityDetailDrawer identity={identity} tenantName="acme" onClose={vi.fn()} />);

    expect(screen.getByTestId("link-renderer")).toHaveTextContent("user-drawer");
  });
});
