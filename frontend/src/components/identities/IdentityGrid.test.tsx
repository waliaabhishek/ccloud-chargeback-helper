import { render } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import { describe, expect, it, vi } from "vitest";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";
import { IdentityGrid } from "./IdentityGrid";

let capturedColumnDefs: ColDef[] | undefined;

vi.mock("ag-grid-react", () => ({
  AgGridReact: ({ columnDefs }: { columnDefs?: ColDef[] }) => {
    capturedColumnDefs = columnDefs;
    return <div data-testid="ag-grid" />;
  },
}));

describe("IdentityGrid", () => {
  it("uses the shared renderer for the identity identifier column", () => {
    render(<IdentityGrid tenantName="acme" queryParams={{}} onRowClick={vi.fn()} />);

    expect(
      capturedColumnDefs?.find((column) => column.field === "identity_id")
        ?.cellRenderer,
    ).toBe(ConfluentLinkRenderer);
  });
});
