import { promises as fs } from "node:fs";
import path from "node:path";
let playwrightModule;
try {
  playwrightModule = await import("/app/node_modules/playwright/index.js");
} catch {
  playwrightModule = await import("playwright");
}
const playwright = playwrightModule.default ?? playwrightModule;
const { chromium } = playwright;

function trimTrailingSlash(value) {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function pushLimited(target, value, limit = 50) {
  if (target.length < limit) {
    target.push(value);
  }
}

async function resolveChromiumExecutable() {
  const browserRoot = "/ms-playwright";
  const entries = await fs.readdir(browserRoot, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith("chromium-")) {
      continue;
    }
    const candidate = path.join(browserRoot, entry.name, "chrome-linux", "chrome");
    try {
      await fs.access(candidate);
      return candidate;
    } catch {
    }
  }
  throw new Error("No Chromium executable was found under /ms-playwright.");
}

async function main() {
  const [, , configPath, resultPath] = process.argv;
  if (!configPath || !resultPath) {
    throw new Error("usage: node preferred_client_playback_proof.mjs <config> <result>");
  }

  const configText = (await fs.readFile(configPath, "utf8")).replace(/^\uFEFF/, "");
  const config = JSON.parse(configText);
  const frontendUrl = trimTrailingSlash(String(config.frontendUrl));
  const detailUrl = String(config.detailUrl);
  const itemId = String(config.itemId ?? "");
  const loginUrl = `${frontendUrl}/auth/login`;
  const screenshotPath = String(config.screenshotPath);
  const playbackTimeoutMs = Number(config.playbackTimeoutMs ?? 90000);
  const browserExecutablePath =
    typeof config.browserExecutablePath === "string" && config.browserExecutablePath.trim()
      ? config.browserExecutablePath.trim()
      : null;
  const browserCdpUrl =
    typeof config.browserCdpUrl === "string" && config.browserCdpUrl.trim()
      ? config.browserCdpUrl.trim()
      : null;
  const hlsParams = new URLSearchParams({
    pix_fmt: "yuv420p",
    profile: "high",
    level: "4.1",
  });
  const hlsManifestUrl = `${frontendUrl}/api/stream/${itemId}/hls/index.m3u8?${hlsParams.toString()}`;

  if (!itemId) {
    throw new Error("itemId is required for preferred-client playback proof");
  }

  const consoleMessages = [];
  const pageErrors = [];
  const requestFailures = [];
  const streamResponses = [];
  const result = {
    status: "failed",
    frontend_url: frontendUrl,
    login_url: loginUrl,
    detail_url: detailUrl,
    item_id: itemId,
    hls_manifest_url: hlsManifestUrl,
    final_url: null,
    playback_started: false,
    playback_mode: "unknown",
    current_time_seconds: 0,
    duration_seconds: 0,
    ready_state: 0,
    paused: true,
    ended: false,
    error_text: null,
    current_src: null,
    hls_prewarm_status: null,
    hls_prewarm_attempts: 0,
    hls_prewarm_error: null,
    console_messages: consoleMessages,
    page_errors: pageErrors,
    request_failures: requestFailures,
    stream_responses: streamResponses,
  };

  let browser;
  let page;
  try {
    if (browserCdpUrl) {
      browser = await chromium.connectOverCDP(browserCdpUrl);
      const [connectedContext] = browser.contexts();
      const context = connectedContext ?? (await browser.newContext({
        viewport: { width: 1600, height: 1000 },
        ignoreHTTPSErrors: true,
      }));
      page = await context.newPage();
    } else {
      const executablePath = browserExecutablePath ?? (await resolveChromiumExecutable());
      browser = await chromium.launch({
        executablePath,
        headless: true,
        args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required", "--host-resolver-rules=MAP localhost host.docker.internal,MAP 127.0.0.1 host.docker.internal"],
      });
      const context = await browser.newContext({
        viewport: { width: 1600, height: 1000 },
        ignoreHTTPSErrors: true,
      });
      page = await context.newPage();
    }

    page.on("console", (message) => {
      pushLimited(consoleMessages, {
        type: message.type(),
        text: message.text(),
      });
    });
    page.on("pageerror", (error) => {
      pushLimited(pageErrors, error.message);
    });
    page.on("requestfailed", (request) => {
      if (
        request.url().includes("/api/stream/") ||
        request.url().includes("/api/v1/stream/")
      ) {
        pushLimited(requestFailures, {
          url: request.url(),
          method: request.method(),
          failure: request.failure()?.errorText ?? "unknown",
          resource_type: request.resourceType(),
        });
      }
    });
    page.on("response", (response) => {
      const url = response.url();
      if (!url.includes("/api/stream/") && !url.includes("/api/v1/stream/")) {
        return;
      }
      pushLimited(streamResponses, {
        url,
        status: response.status(),
        method: response.request().method(),
        resource_type: response.request().resourceType(),
      });
    });

    await page.goto(loginUrl, { waitUntil: "networkidle", timeout: playbackTimeoutMs });
    await page.locator('input[autocomplete*="username"]').fill(String(config.username));
    await page.locator('input[type="password"]').fill(String(config.password));
    await page.getByRole("button", { name: "Submit" }).click();
    await page.waitForURL((url) => !url.pathname.startsWith("/auth/login"), {
      timeout: playbackTimeoutMs,
    });

    const hlsPrewarm = {
      ok: false,
      status: null,
      attempts: 0,
      error: null,
    };
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      hlsPrewarm.attempts = attempt;
      try {
        const response = await page.evaluate(async ({ url, timeoutMs }) => {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
          try {
            const manifestResponse = await fetch(url, {
              credentials: "include",
              signal: controller.signal,
            });
            const body = await manifestResponse.text();
            return {
              status: manifestResponse.status,
              body,
            };
          } finally {
            clearTimeout(timer);
          }
        }, {
          url: hlsManifestUrl,
          timeoutMs: playbackTimeoutMs,
        });
        hlsPrewarm.status = response.status;
        if (response.status >= 200 && response.status < 300 && response.body.includes("#EXTM3U")) {
          hlsPrewarm.ok = true;
          hlsPrewarm.error = null;
          break;
        }
        hlsPrewarm.error = response.body.slice(0, 300) || `Unexpected HLS manifest response (${response.status})`;
      } catch (error) {
        hlsPrewarm.error = error instanceof Error ? error.message : String(error);
      }
      await page.waitForTimeout(3000);
    }
    result.hls_prewarm_status = hlsPrewarm.status;
    result.hls_prewarm_attempts = hlsPrewarm.attempts;
    result.hls_prewarm_error = hlsPrewarm.error;

    await page.goto(detailUrl, { waitUntil: "domcontentloaded", timeout: playbackTimeoutMs });

    let playAttempt = { ok: false, error: "video element not found" };
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await page.waitForSelector("video", { state: "attached", timeout: playbackTimeoutMs });
      await page.waitForTimeout(750);

      playAttempt = await page.evaluate(async () => {
        const video = document.querySelector("video");
        if (!(video instanceof HTMLVideoElement)) {
          return { ok: false, error: "video element not found", retryable: true };
        }

        video.muted = true;
        video.defaultMuted = true;
        try {
          await video.play();
          return { ok: true, error: null, retryable: false };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          return {
            ok: false,
            error: message,
            retryable:
              message.includes("removed from the document") ||
              message.includes("pause() request was interrupted") ||
              message.includes("not allowed") ||
              message.includes("AbortError"),
          };
        }
      });

      if (playAttempt.ok) {
        break;
      }
      if (!playAttempt.retryable || attempt === 4) {
        break;
      }
    }

    if (!playAttempt.ok) {
      throw new Error(`video.play() failed: ${playAttempt.error}`);
    }

    await page.waitForFunction(() => {
      const video = document.querySelector("video");
      return (
        video instanceof HTMLVideoElement &&
        !video.paused &&
        Number.isFinite(video.currentTime) &&
        video.currentTime >= 2
      );
    }, { timeout: playbackTimeoutMs });

    await page.screenshot({ path: screenshotPath, fullPage: true });

    const videoState = await page.evaluate(() => {
      const video = document.querySelector("video");
      const errorNode = document.querySelector("div p");
      if (!(video instanceof HTMLVideoElement)) {
        return {
          playback_started: false,
          current_time_seconds: 0,
          duration_seconds: 0,
          ready_state: 0,
          paused: true,
          ended: false,
          current_src: null,
          error_text: errorNode?.textContent?.trim() ?? null,
        };
      }
      return {
        playback_started: !video.paused && video.currentTime > 0,
        current_time_seconds: video.currentTime,
        duration_seconds: Number.isFinite(video.duration) ? video.duration : 0,
        ready_state: video.readyState,
        paused: video.paused,
        ended: video.ended,
        current_src: video.currentSrc || video.src || null,
        error_text: errorNode?.textContent?.trim() ?? null,
      };
    });

    result.final_url = page.url();
    result.playback_started = Boolean(videoState.playback_started);
    result.current_time_seconds = Number(videoState.current_time_seconds ?? 0);
    result.duration_seconds = Number(videoState.duration_seconds ?? 0);
    result.ready_state = Number(videoState.ready_state ?? 0);
    result.paused = Boolean(videoState.paused);
    result.ended = Boolean(videoState.ended);
    result.current_src = videoState.current_src;
    result.error_text = videoState.error_text;

    const sawHls = streamResponses.some((entry) => entry.url.includes("/hls/"));
    const sawDirect = streamResponses.some(
      (entry) => entry.url.includes("/api/stream/") && !entry.url.includes("/hls/"),
    );
    result.playback_mode = sawHls ? "hls" : sawDirect ? "direct" : "unknown";
    result.status =
      result.playback_started &&
      result.current_time_seconds >= 2 &&
      streamResponses.some((entry) => entry.status >= 200 && entry.status < 300)
        ? "playing"
        : "failed";
  } catch (error) {
    result.status = "failed";
    result.final_url = page?.url?.() ?? result.final_url;
    result.error_text = error instanceof Error ? error.message : String(error);
    if (page && screenshotPath) {
      try {
        await page.screenshot({ path: screenshotPath, fullPage: true });
      } catch {
      }
    }
  } finally {
    if (browser) {
      await browser.close();
    }
    await fs.mkdir(path.dirname(resultPath), { recursive: true });
    await fs.writeFile(resultPath, JSON.stringify(result, null, 2), "utf8");
  }

  if (result.status !== "playing") {
    process.exitCode = 1;
  }
}

await main();

