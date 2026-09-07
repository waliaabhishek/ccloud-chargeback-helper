import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConfluentLinkRenderer } from "./ConfluentLinkRenderer";

// ---------------------------------------------------------------------------
// Mock ResourceLinkContext so ConfluentLinkRenderer tests control feature flag
// ---------------------------------------------------------------------------

const mockResolveUrl = vi.fn<(id: string) => string | null>();
const mockRegisterIdentifier = vi.fn<(id: string) => () => void>(() => vi.fn());
let mockEnabled = true;

vi.mock("../../providers/ResourceLinkContext", () => ({
  useResourceLinks: vi.fn(() => ({
    enabled: mockEnabled,
    resolveUrl: mockResolveUrl,
    registerIdentifier: mockRegisterIdentifier,
    setEnabled: vi.fn(),
    isLoading: false,
  })),
  ResourceLinkProvider: ({ children }: { children: ReactNode }) => (
    <>{children}</>
  ),
}));

afterEach(() => {
  vi.clearAllMocks();
  mockRegisterIdentifier.mockReturnValue(vi.fn());
  mockEnabled = true;
});

// ---------------------------------------------------------------------------
// ConfluentLinkRenderer — cell renderer (resolves URL from context)
// ---------------------------------------------------------------------------

describe("ConfluentLinkRenderer", () => {
  it("renders link when resolveUrl returns a URL", () => {
    mockResolveUrl.mockReturnValue(
      "https://confluent.cloud/environments/env-abc123",
    );

    render(<ConfluentLinkRenderer value="env-abc123" />);

    const link = screen.getByRole("link");
    expect(link.getAttribute("href")).toBe(
      "https://confluent.cloud/environments/env-abc123",
    );
    expect(link.textContent).toBe("env-abc123");
  });

  it("renders plain text when resolveUrl returns null", () => {
    mockResolveUrl.mockReturnValue(null);

    render(<ConfluentLinkRenderer value="lkc-notfound" />);

    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByText("lkc-notfound")).toBeTruthy();
  });

  it.each([null, undefined])("renders '—' without registering when value is %s", (value) => {
    render(<ConfluentLinkRenderer value={value} />);

    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByText("—")).toBeTruthy();
    expect(mockRegisterIdentifier).not.toHaveBeenCalled();
  });

  it("feature flag off renders plain text even when resolveUrl would return URL", () => {
    mockEnabled = false;
    mockResolveUrl.mockReturnValue(
      "https://confluent.cloud/environments/env-abc123",
    );

    render(<ConfluentLinkRenderer value="env-abc123" />);

    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByText("env-abc123")).toBeTruthy();
  });

  it("link has target='_blank' and rel='noopener noreferrer'", () => {
    mockResolveUrl.mockReturnValue(
      "https://confluent.cloud/environments/env-abc123",
    );

    render(<ConfluentLinkRenderer value="env-abc123" />);

    const link = screen.getByRole("link");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("calls resolveUrl with the provided value", () => {
    mockResolveUrl.mockReturnValue(null);

    render(<ConfluentLinkRenderer value="lkc-def456" />);

    expect(mockResolveUrl).toHaveBeenCalledWith("lkc-def456");
  });

  it("registers an enabled unresolved identifier and invokes its cleanup on unmount", () => {
    const cleanup = vi.fn();
    mockResolveUrl.mockReturnValue(null);
    mockRegisterIdentifier.mockReturnValue(cleanup);

    const { unmount } = render(<ConfluentLinkRenderer value="lkc-register" />);

    expect(mockRegisterIdentifier).toHaveBeenCalledTimes(1);
    expect(mockRegisterIdentifier).toHaveBeenCalledWith("lkc-register");
    unmount();
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("does not register disabled, empty, whitespace-only, or direct-url values", () => {
    mockEnabled = false;
    const { rerender } = render(<ConfluentLinkRenderer value="env-disabled" />);
    rerender(<ConfluentLinkRenderer value={null} />);
    rerender(<ConfluentLinkRenderer value="   " />);
    rerender(
      <ConfluentLinkRenderer
        value="topic-direct"
        url="https://confluent.cloud/environments/env-1/clusters/lkc-1/topics/topic-direct"
      />,
    );

    expect(mockRegisterIdentifier).not.toHaveBeenCalled();
  });

  it("works with direct url prop bypassing resolveUrl (topic attribution pattern)", () => {
    // Topic attribution grids may supply the URL directly without index lookup
    render(
      <ConfluentLinkRenderer
        value="orders-topic"
        url="https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/topics/orders-topic"
      />,
    );

    const link = screen.getByRole("link");
    expect(link.getAttribute("href")).toBe(
      "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/topics/orders-topic",
    );
    expect(mockRegisterIdentifier).not.toHaveBeenCalled();
  });
});
