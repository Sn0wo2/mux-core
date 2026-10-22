#!/usr/bin/env node
const { createHash } = require("node:crypto")
const { spawnSync } = require("node:child_process")
const {
  appendFileSync,
  createReadStream,
  existsSync,
  mkdirSync,
  readdirSync,
  writeFileSync,
} = require("node:fs")

function appendToGithubFile(name, value) {
  const file = process.env[name]
  if (!file) throw new Error(`${name} is not set`)
  appendFileSync(file, value)
}

function releaseFiles() {
  const entries = readdirSync("dist").filter((name) => !name.startsWith("."))
  return [
    ...entries.filter((name) => name.endsWith(".tar.gz")).sort(),
    ...entries.filter((name) => name.endsWith(".zip")).sort(),
  ]
}

async function main() {
  switch (process.argv[2]) {
    case "normalize-version": {
      const releaseVersion = process.env.RELEASE_VERSION
      const version = releaseVersion.startsWith("v") ? releaseVersion.slice(1) : releaseVersion
      const semver = /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$/
      if (!semver.test(version)) {
        console.error(`Invalid semantic version: ${releaseVersion}`)
        process.exit(1)
      }
      appendToGithubFile("GITHUB_ENV", `VERSION=${version}\n`)
      appendToGithubFile("GITHUB_OUTPUT", `version=${version}\n`)
      break
    }

    case "verify-metadata": {
      const metadata = JSON.parse(
        spawnSync("cargo", ["metadata", "--no-deps", "--format-version", "1"], {
          encoding: "utf8",
        }).stdout,
      )
      const workspaceMembers = new Set(metadata.workspace_members)
      const packages = metadata.packages.filter((pkg) => workspaceMembers.has(pkg.id))
      const publishable = packages.filter(
        (pkg) => pkg.publish == null || pkg.publish.includes("crates-io"),
      )
      const released = packages.filter(
        (pkg) =>
          publishable.includes(pkg) ||
          (metadata.workspace_default_members.includes(pkg.id) &&
            pkg.targets.some((target) => target.kind.includes("bin"))),
      )
      const mismatches =
        process.env.VERIFY_VERSION === "true"
          ? released.filter((pkg) => pkg.version !== process.env.VERSION)
          : []
      if (mismatches.length) {
        console.error(`Expected released packages to have version ${process.env.VERSION}:`)
        for (const pkg of mismatches) console.error(`  ${pkg.name} ${pkg.version}`)
        process.exit(1)
      }
      if (
        process.env.GITHUB_EVENT_NAME === "push" &&
        process.env.GITHUB_SHA !==
          spawnSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).stdout.trim()
      ) {
        console.error("Checked out commit does not match the pushed tag commit")
        process.exit(1)
      }
      appendToGithubFile(
        "GITHUB_OUTPUT",
        `has_publishable_crates=${publishable.length ? "true" : "false"}\n`,
      )
      break
    }

    case "generate-checksums": {
      mkdirSync("dist", { recursive: true })
      const files = releaseFiles()
      if (files.length) {
        const lines = []
        for (const name of files) {
          const hash = createHash("sha256")
          for await (const chunk of createReadStream(`dist/${name}`)) hash.update(chunk)
          const digest = hash.digest("hex")
          const escapedName = name
            .replaceAll("\\", "\\\\")
            .replaceAll("\n", "\\n")
            .replaceAll("\r", "\\r")
          lines.push(`${escapedName === name ? "" : "\\"}${digest}  ${escapedName}\n`)
        }
        writeFileSync("dist/checksums.txt", lines.join(""))
      }
      break
    }

    case "create-release": {
      const tag = `v${process.env.VERSION}`
      const prerelease = process.env.VERSION.includes("-")
      const existing = spawnSync("gh", ["release", "view", tag], { stdio: "ignore" })
      if (existing.error) throw existing.error
      if (existing.status === 0) {
        console.error(`Release ${tag} already exists; refusing to modify it`)
        process.exit(1)
      }
      const files = releaseFiles().map((name) => `dist/${name}`)
      if (existsSync("dist/checksums.txt")) files.push("dist/checksums.txt")
      const result = spawnSync(
        "gh",
        [
          "release",
          "create",
          tag,
          ...files,
          "--verify-tag",
          "--generate-notes",
          "--title",
          tag,
          ...(prerelease ? ["--prerelease"] : []),
        ],
        { stdio: "inherit" },
      )
      if (result.error) throw result.error
      if (result.status !== 0) process.exit(result.status ?? 1)
      break
    }

    default:
      throw new Error(`Unknown release command: ${process.argv[2] ?? ""}`)
  }
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
