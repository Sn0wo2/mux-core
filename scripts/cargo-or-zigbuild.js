#!/usr/bin/env node
const { createHash } = require("node:crypto")
const { spawnSync } = require("node:child_process")
const { resolve } = require("node:path")

const rustc = spawnSync("rustc", ["-vV"], { encoding: "utf8" })
if (rustc.status !== 0) {
  console.error(`ERROR: rustc -vV failed: ${rustc.stderr}`)
  process.exit(1)
}
const host = rustc.stdout.match(/host:\s+(\S+)/)?.[1]
if (!host) {
  console.error("ERROR: Could not determine host target from rustc -vV")
  process.exit(1)
}

let args = process.argv.slice(2)
let target = null
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--target" && i + 1 < args.length) {
    target = args[i + 1]
    break
  }
  if (args[i].startsWith("--target=")) {
    target = args[i].slice("--target=".length)
    break
  }
}

let subcommand = ""
let env = process.env
if (target === "x86_64-pc-windows-gnu" && target !== host) {
  env = {
    ...env,
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:
      process.env.CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER ?? "x86_64-w64-mingw32-gcc",
  }
} else if (target && target !== host) {
  subcommand = "zigbuild"
  env = {
    ...env,
    CARGO_ZIGBUILD_CACHE_DIR: resolve(
      "target",
      "zigbuild",
      target,
      createHash("sha256").update(process.env.RUSTFLAGS ?? "").digest("hex"),
    ),
  }
  if (args[0] === "build") args = args.slice(1)
}

process.exit(
  spawnSync("cargo", [...(subcommand ? [subcommand] : []), ...args], {
    stdio: "inherit",
    env,
  }).status ?? 1,
)
