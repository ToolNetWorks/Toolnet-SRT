import { describe, it, expect } from "vitest";
import * as index from "./index.js";

describe("jianying-subtitle exports", () => {
  it("exports transcribe function", () => {
    expect(typeof index.transcribe).toBe("function");
  });

  it("exports toSrt function", () => {
    expect(typeof index.toSrt).toBe("function");
  });

  it("exports toJson function", () => {
    expect(typeof index.toJson).toBe("function");
  });

  it("exports writeSubtitle function", () => {
    expect(typeof index.writeSubtitle).toBe("function");
  });

  it("exports writeSubtitles function", () => {
    expect(typeof index.writeSubtitles).toBe("function");
  });
});
