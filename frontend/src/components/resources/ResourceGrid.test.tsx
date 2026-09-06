import { render } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import { describe, expect, it, vi } from "vitest";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";
import { ResourceGrid } from "./ResourceGrid";

let capturedColumnDefs: ColDef[] | undefined;

vi.mock("ag-grid-react", () => ({
  AgGridReact: ({ columnDefs }: { columnDefs?: ColDef[] }) => {
    capturedColumnDefs = columnDefs;
    return <div data-testid="ag-grid" />;
  },
}));

describe("ResourceGrid", () => {
  it("uses the shared renderer for the resource identifier column", () => {
    render(<ResourceGrid tenantName="acme" queryParams={{}} onRowClick={vi.fn()} />);

    expect(
      capturedColumnDefs?.find((column) => column.field === "resource_id")
        ?.cellRenderer,
    ).toBe(ConfluentLinkRenderer);
  });
});
