import { render } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import { describe, expect, it, vi } from "vitest";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";
import { BillingGrid } from "./BillingGrid";

let capturedColumnDefs: ColDef[] | undefined;

vi.mock("ag-grid-react", () => ({
  AgGridReact: ({ columnDefs }: { columnDefs?: ColDef[] }) => {
    capturedColumnDefs = columnDefs;
    return <div data-testid="ag-grid" />;
  },
}));

describe("BillingGrid", () => {
  it("uses the shared renderer for its resource column", () => {
    render(<BillingGrid tenantName="acme" filters={{}} />);

    expect(
      capturedColumnDefs?.find((column) => column.field === "resource_id")
        ?.cellRenderer,
    ).toBe(ConfluentLinkRenderer);
  });
});
