#!/usr/bin/env node
const { execFileSync } = require("node:child_process")
const { appendFileSync, mkdirSync, writeFileSync } = require("node:fs")
const { basename, dirname, join, relative } = require("node:path")

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version", "1"], {
    encoding: "utf8",
  }),
)
const packages = metadata.packages.filter((pkg) =>
  metadata.workspace_default_members.includes(pkg.id),
)
const binaries = packages.flatMap((pkg) =>
  pkg.targets
    .filter((target) => target.kind.includes("bin"))
    .map((target) => ({ package: pkg.name, name: target.name })),
)
if (new Set(binaries.map(({ name }) => name)).size !== binaries.length) {
  throw new Error("Duplicate release binary name")
}

const platforms = [
  ["linux-amd64-v2", "x86_64-unknown-linux-gnu", "linux-windows", "x86-64-v2"],
  ["linux-amd64-v3", "x86_64-unknown-linux-gnu", "linux-windows", "x86-64-v3"],
  ["linux-arm64", "aarch64-unknown-linux-gnu", "linux-windows"],
  ["linux-armv7", "armv7-unknown-linux-gnueabihf", "linux-windows"],
  ["windows-amd64-v2", "x86_64-pc-windows-gnu", "linux-windows", "x86-64-v2"],
  ["windows-amd64-v3", "x86_64-pc-windows-gnu", "linux-windows", "x86-64-v3"],
  ["darwin-amd64-v2", "x86_64-apple-darwin", "macos", "x86-64-v2"],
  ["darwin-amd64-v3", "x86_64-apple-darwin", "macos", "x86-64-v3"],
  ["darwin-arm64", "aarch64-apple-darwin", "macos"],
]
const config = {
  version: 2,
  project_name: process.env.GITHUB_REPOSITORY?.split("/").pop() ??
    (packages.length === 1 ? packages[0].name : basename(metadata.workspace_root)),
  builds: binaries.length ? [] : [{ skip: true }],
  source: { enabled: false },
  archives: [],
  checksum: { name_template: "checksums.txt" },
  snapshot: { version_template: "{{ .Version }}-next" },
  changelog: { disable: true },
}
if (process.env.BUILD_PLATFORM !== "macos") {
  config.archives.push({
    id: "source",
    meta: true,
    formats: ["tar.gz"],
    name_template: "{{ .ProjectName }}_{{ .Version }}_source",
    files: [
      "src/**", "crates/**", "tests/**", "benches/**", "examples/**",
      ".cargo/**", "build.r[s]", "Cargo.toml", "Cargo.loc[k]", "README.m[d]", "LICENS[E]",
      ...metadata.packages
        .filter((pkg) => metadata.workspace_members.includes(pkg.id))
        .map((pkg) => {
          const directory = relative(metadata.workspace_root, dirname(pkg.manifest_path))
          return directory && !directory.startsWith("..")
            ? `${directory.replaceAll("\\", "/")}/**`
            : null
        })
        .filter(Boolean),
    ],
  })
}
for (const [id, target, runner, cpu] of platforms) {
  if (!binaries.length || (process.env.BUILD_PLATFORM && process.env.BUILD_PLATFORM !== runner)) {
    continue
  }
  const ids = []
  for (const binary of binaries) {
    const buildId = `${id}-${binary.name}`
    ids.push(buildId)
    config.builds.push({
      id: buildId,
      builder: "rust",
      command: "build",
      tool: runner === "macos" ? "cargo" : "./scripts/cargo-or-zigbuild.js",
      binary: binary.name,
      targets: [target],
      flags: ["--release", `--package=${binary.package}`, `--bin=${binary.name}`],
      ...(cpu ? { env: [`RUSTFLAGS=${process.env.RUSTFLAGS ?? ""} -C target-cpu=${cpu}`] } : {}),
    })
  }
  config.archives.push({
    id,
    ids,
    name_template: `{{ .ProjectName }}_{{ .Version }}_${id.replaceAll("-", "_")}`,
    formats: [id.startsWith("windows") ? "zip" : "tar.gz"],
    files: ["LICENS[E]", "README.m[d]"],
  })
}
mkdirSync("target", { recursive: true })
writeFileSync(join("target", "goreleaser.json"), `${JSON.stringify(config, null, 2)}\n`)
if (process.env.GITHUB_ENV) {
  appendFileSync(process.env.GITHUB_ENV, `RELEASE_BINARIES=${JSON.stringify(binaries)}\n`)
}
