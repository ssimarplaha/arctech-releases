// Reviewed public-repository broker. Builtins only; downloaded source artifacts
// are treated as data and are never executed by the privileged publishing job.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import { createReadStream, createWriteStream, constants } from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export const SOURCE_REPOSITORY = "ssimarplaha/arctech";
export const DISTRIBUTION_REPOSITORY = "ssimarplaha/arctech-releases";
const arches = ["arm64", "x64"];
const json = async (file) => JSON.parse(await fs.readFile(file, "utf8"));
export async function checksum(file) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest("hex");
}
export async function qualifiedReleaseAssets(
  directory,
  expectedCommit,
  { publicReports = false } = {},
) {
  const manifests = await Promise.all(
    arches.map((arch) =>
      json(path.join(directory, `arctech-${arch}.manifest.json`)),
    ),
  );
  const [manifest] = manifests;
  assert.match(
    manifest.version,
    /^\d+\.\d+\.\d+$/,
    "numeric application version required",
  );
  assert.match(
    expectedCommit ?? "",
    /^[a-f0-9]{40}$/,
    "expected source commit required",
  );
  for (const [index, arch] of arches.entries()) {
    const candidate = manifests[index];
    if (
      candidate.name !== "arctech" ||
      candidate.schemaVersion !== 1 ||
      candidate.platform !== "darwin" ||
      candidate.minimumMacOS !== "15.0" ||
      candidate.nodeVersion !== "24.22.0" ||
      candidate.version !== manifest.version ||
      candidate.sourceCommit !== expectedCommit ||
      candidate.sourceDirty !== false ||
      candidate.arch !== arch ||
      candidate.archiveRoot !== `arctech-v${manifest.version}-darwin-${arch}`
    )
      throw new Error(
        "Clean source, source commit, versions and both native architectures must agree",
      );
  }
  const required = ["install.sh", "qualification.json"];
  for (const arch of arches)
    for (const suffix of [
      "tar.gz",
      "tar.gz.sha256",
      "manifest.json",
      "licenses.json",
    ])
      required.push(`arctech-v${manifest.version}-darwin-${arch}.${suffix}`);
  const approved = new Set([
    ...required,
    "arctech-arm64.manifest.json",
    "arctech-x64.manifest.json",
    "SHA256SUMS",
    ...(publicReports ? arches.map((arch) => `public-${arch}.json`) : []),
  ]);
  const files = await fs.readdir(directory);
  for (const name of files) {
    if (!approved.has(name))
      throw new Error(`Unexpected file in public asset set: ${name}`);
    if (!(await fs.lstat(path.join(directory, name))).isFile())
      throw new Error(
        `Only regular allowlisted asset files may be published: ${name}`,
      );
  }
  for (const name of required)
    if (!files.includes(name)) throw new Error(`Missing release asset ${name}`);
  const archiveChecksums = {};
  for (const arch of arches) {
    const archive = `arctech-v${manifest.version}-darwin-${arch}.tar.gz`;
    const digest = await checksum(path.join(directory, archive));
    archiveChecksums[arch] = digest;
    if (
      (
        await fs.readFile(path.join(directory, `${archive}.sha256`), "utf8")
      ).trim() !== `${digest}  ${archive}`
    )
      throw new Error(`Archive checksum mismatch: ${arch}`);
    const alias = await fs.readFile(
      path.join(directory, `arctech-${arch}.manifest.json`),
    );
    if (
      !alias.equals(
        await fs.readFile(
          path.join(
            directory,
            `arctech-v${manifest.version}-darwin-${arch}.manifest.json`,
          ),
        ),
      )
    )
      throw new Error(
        `Uploaded manifest disagrees with checked manifest: ${arch}`,
      );
  }
  const qualification = await json(path.join(directory, "qualification.json"));
  for (const [kind, smoke] of [
    ["reports", true],
    ["acceptance", false],
  ]) {
    if (!Array.isArray(qualification[kind]) || qualification[kind].length !== 4)
      throw new Error(`Exactly four native ${kind} reports are required`);
    for (const arch of arches)
      for (const major of [15, 26]) {
        const matching = qualification[kind].filter(
          (report) =>
            report.arch === arch &&
            typeof report.macOS === "string" &&
            report.macOS.startsWith(`${major}.`),
        );
        const report = matching[0];
        if (
          matching.length !== 1 ||
          report.success !== true ||
          report.sourceCommit !== expectedCommit ||
          report.version !== manifest.version ||
          !Array.isArray(report.checks) ||
          !report.checks.length ||
          (smoke
            ? report.smoke !== true
            : !Array.isArray(report.unverified) ||
              report.unverified.length !== 0)
        )
          throw new Error(
            `Missing native macOS ${major} ${arch} ${kind} qualification`,
          );
        if (!smoke && report.archiveSHA256 !== archiveChecksums[arch])
          throw new Error(
            `Acceptance archive checksum mismatch: macOS ${major} ${arch}`,
          );
      }
  }
  if (publicReports)
    for (const arch of arches) {
      const report = await json(path.join(directory, `public-${arch}.json`));
      if (
        report.success !== true ||
        report.smoke !== true ||
        report.arch !== arch ||
        report.version !== manifest.version ||
        report.sourceCommit !== expectedCommit
      )
        throw new Error(`Anonymous download qualification missing: ${arch}`);
    }
  const checksums = {};
  for (const name of required.sort())
    checksums[name] = await checksum(path.join(directory, name));
  return { manifest, required: required.sort(), checksums };
}
export async function assembleArtifacts(source, destination, expectedCommit) {
  await fs.mkdir(destination, { recursive: true });
  if ((await fs.readdir(destination)).length)
    throw new Error(
      "Public asset output must be empty; preserve it for review and select a fresh directory",
    );
  const reports = [],
    acceptance = [];
  let installer;
  for (const directory of (await fs.readdir(source)).sort()) {
    const full = path.join(source, directory);
    if (!(await fs.lstat(full)).isDirectory())
      throw new Error(`Unexpected source artifact entry: ${directory}`);
    for (const name of (await fs.readdir(full)).sort()) {
      const file = path.join(full, name);
      if (!(await fs.lstat(file)).isFile())
        throw new Error(
          `Source artifact must contain regular files only: ${name}`,
        );
      if (name === "install.sh") {
        const bytes = await fs.readFile(file);
        if (installer && !bytes.equals(installer))
          throw new Error("Architecture installers disagree");
        installer = bytes;
        await fs.copyFile(file, path.join(destination, name));
      } else if (
        name.endsWith(".qualification.json") ||
        /^qualification-26-(?:arm64|x64)\.json$/.test(name)
      )
        reports.push(await json(file));
      else if (/^acceptance-(?:15|26)-(?:arm64|x64)\.json$/.test(name))
        acceptance.push(await json(file));
      else if (/^public-(?:arm64|x64)\.json$/.test(name))
        await fs.copyFile(file, path.join(destination, name));
      else if (
        /^arctech-v\d+\.\d+\.\d+-darwin-(?:arm64|x64)\.(?:tar\.gz(?:\.sha256)?|manifest\.json|licenses\.json)$/.test(
          name,
        )
      ) {
        await fs.copyFile(
          file,
          path.join(destination, name),
          constants.COPYFILE_EXCL,
        );
        if (name.endsWith(".manifest.json")) {
          const manifest = await json(file);
          if (!arches.includes(manifest.arch))
            throw new Error("Invalid manifest architecture");
          await fs.copyFile(
            file,
            path.join(destination, `arctech-${manifest.arch}.manifest.json`),
            constants.COPYFILE_EXCL,
          );
        }
      } else throw new Error(`Unexpected file in source artifacts: ${name}`);
    }
  }
  await fs.writeFile(
    path.join(destination, "qualification.json"),
    JSON.stringify({ schemaVersion: 1, reports, acceptance }, null, 2) + "\n",
  );
  return (
    await qualifiedReleaseAssets(destination, expectedCommit, {
      publicReports: (
        await fs.readdir(destination)
      ).some((name) => name.startsWith("public-")),
    })
  ).manifest;
}
function client(fetchImpl) {
  return async (
    endpoint,
    token,
    { allowNotFound = false, ...options } = {},
  ) => {
    const url = new URL(
      endpoint.startsWith("https:")
        ? endpoint
        : `https://api.github.com${endpoint}`,
    );
    if (
      !["https://api.github.com", "https://uploads.github.com"].includes(
        url.origin,
      )
    )
      throw new Error("Refusing to send a GitHub credential to another origin");
    const response = await fetchImpl(url.href, {
      ...options,
      redirect: "error",
      headers: {
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        "X-GitHub-Api-Version": "2026-03-10",
        ...options.headers,
      },
    });
    if (allowNotFound && response.status === 404) return null;
    if (!response.ok)
      throw new Error(
        `GitHub API ${response.status} for ${url.pathname}.${
          response.status === 422
            ? " Recover an unpublished draft manually before retrying; never replace a published immutable release."
            : " Verify the protected environment and narrowly scoped token permissions."
        }`,
      );
    return response.status === 204 ? null : response.json();
  };
}
export async function publishRelease(
  mode,
  directory,
  env = process.env,
  fetchImpl = fetch,
) {
  if (!["stage", "promote"].includes(mode))
    throw new Error("Expected stage or promote");
  if (env.GITHUB_REPOSITORY !== DISTRIBUTION_REPOSITORY || !env.GITHUB_TOKEN)
    throw new Error(
      "Run publication in the public distribution repository's protected workflow",
    );
  if (!env.MACOS_RELEASE_POLICY_TOKEN)
    throw new Error(
      "Configure MACOS_RELEASE_POLICY_TOKEN with Administration read only on the public distribution repository; the ephemeral writer needs no administration permission",
    );
  const { manifest, required, checksums } = await qualifiedReleaseAssets(
    directory,
    env.SOURCE_COMMIT,
    { publicReports: mode === "promote" },
  );
  const api = client(fetchImpl),
    writer = env.GITHUB_TOKEN;
  const environment = await api(
    `/repos/${DISTRIBUTION_REPOSITORY}/environments/macos-release`,
    writer,
  );
  if (
    !environment.protection_rules?.some(
      (rule) => rule.type === "required_reviewers" && rule.reviewers?.length,
    )
  )
    throw new Error(
      "macos-release must have a required reviewer configured before publishing",
    );
  const repo = await api(`/repos/${DISTRIBUTION_REPOSITORY}`, writer);
  if (repo.private || repo.visibility !== "public")
    throw new Error(
      "Distribution repository must already be public; the broker never changes visibility",
    );
  // Official REST docs require Administration read. Keep policy access separate
  // from the ephemeral Contents writer, and never change repository settings.
  const immutable = await api(
    `/repos/${DISTRIBUTION_REPOSITORY}/immutable-releases`,
    env.MACOS_RELEASE_POLICY_TOKEN,
  );
  if (!immutable.enabled)
    throw new Error(
      "An administrator must enable immutable releases before publishing",
    );
  await fs.writeFile(
    path.join(directory, "SHA256SUMS"),
    required.map((name) => `${checksums[name]}  ${name}`).join("\n") + "\n",
  );
  const names = [...required, "SHA256SUMS"].sort();
  const digests = {
    ...checksums,
    SHA256SUMS: await checksum(path.join(directory, "SHA256SUMS")),
  };
  const tag = `v${manifest.version}`;
  function sameAssets(release) {
    if (
      release.tag_name !== tag ||
      release.immutable !== true ||
      release.draft !== false ||
      !Array.isArray(release.assets) ||
      release.assets.length !== names.length
    )
      throw new Error(
        "Existing release is not the identical immutable qualified asset set; use a new version",
      );
    for (const name of names) {
      const found = release.assets.filter((asset) => asset.name === name);
      if (
        found.length !== 1 ||
        found[0].digest !== `sha256:${digests[name]}` ||
        found[0].state !== "uploaded"
      )
        throw new Error(
          `Existing immutable release asset differs: ${name}; use a new version`,
        );
    }
  }
  const existing = await api(
    `/repos/${DISTRIBUTION_REPOSITORY}/releases/tags/${tag}`,
    writer,
    { allowNotFound: true },
  );
  let result = existing;
  if (mode === "stage") {
    if (existing) {
      if (existing.draft)
        throw new Error(
          "An unpublished draft exists. Recover it manually before retrying; never replace a published immutable release",
        );
      sameAssets(existing);
      if (!existing.prerelease)
        throw new Error(
          "This version is already stable; use a new version for another release",
        );
    } else {
      const release = await api(
        `/repos/${DISTRIBUTION_REPOSITORY}/releases`,
        writer,
        {
          method: "POST",
          body: JSON.stringify({
            tag_name: tag,
            name: `Arctech ${manifest.version}`,
            draft: true,
            prerelease: true,
            body: `Native macOS 15+ arm64 and Intel distributions.\n\nPrivate source commit: ${manifest.sourceCommit}\n\nBoth archives passed native macOS 15 and 26 qualification. Anonymous public installation is checked by this publishing workflow before optional stable promotion. Runtime, fonts, browser and dependency notices are included in the archives.`,
            make_latest: "false",
          }),
        },
      );
      if (
        !Number.isSafeInteger(release.id) ||
        release.id <= 0 ||
        release.upload_url?.replace(/\{.*$/, "") !==
          `https://uploads.github.com/repos/${DISTRIBUTION_REPOSITORY}/releases/${release.id}/assets`
      )
        throw new Error("Unexpected GitHub release upload target");
      for (const name of names)
        await api(
          `${release.upload_url.replace(/\{.*$/, "")}?name=${encodeURIComponent(
            name,
          )}`,
          writer,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/octet-stream",
              "Content-Length": String(
                (
                  await fs.stat(path.join(directory, name))
                ).size,
              ),
            },
            body: createReadStream(path.join(directory, name)),
            duplex: "half",
          },
        );
      result = await api(
        `/repos/${DISTRIBUTION_REPOSITORY}/releases/${release.id}`,
        writer,
        {
          method: "PATCH",
          body: JSON.stringify({
            draft: false,
            prerelease: true,
            make_latest: "false",
          }),
        },
      );
      sameAssets(result);
      if (result.prerelease !== true)
        throw new Error(
          "GitHub did not publish this version as a prerelease; stable promotion is blocked",
        );
    }
  } else {
    if (!existing)
      throw new Error("No published prerelease exists for this version");
    sameAssets(existing);
    if (existing.prerelease)
      result = await api(
        `/repos/${DISTRIBUTION_REPOSITORY}/releases/${existing.id}`,
        writer,
        {
          method: "PATCH",
          body: JSON.stringify({ prerelease: false, make_latest: "true" }),
        },
      );
    sameAssets(result);
    if (result.prerelease !== false)
      throw new Error("Stable promotion was not confirmed by GitHub");
  }
  return {
    version: manifest.version,
    release_id: result.id,
    source_commit: manifest.sourceCommit,
  };
}
export async function downloadSourceArtifacts(
  runID,
  destination,
  env = process.env,
  fetchImpl = fetch,
) {
  if (!/^\d+$/.test(runID ?? ""))
    throw new Error("A numeric successful private source run ID is required");
  if (
    env.GITHUB_REPOSITORY !== DISTRIBUTION_REPOSITORY ||
    !env.ARCTECH_SOURCE_READ_TOKEN
  )
    throw new Error(
      "Configure ARCTECH_SOURCE_READ_TOKEN with Actions read only on the private source repository",
    );
  const api = client(fetchImpl),
    token = env.ARCTECH_SOURCE_READ_TOKEN;
  const workflow = await api(
    `/repos/${SOURCE_REPOSITORY}/actions/workflows/mac-release.yml`,
    token,
  );
  const run = await api(
    `/repos/${SOURCE_REPOSITORY}/actions/runs/${runID}`,
    token,
  );
  if (
    run.conclusion !== "success" ||
    run.status !== "completed" ||
    run.workflow_id !== workflow.id ||
    workflow.path !== ".github/workflows/mac-release.yml" ||
    run.head_repository?.full_name !== SOURCE_REPOSITORY ||
    !/^[a-f0-9]{40}$/.test(run.head_sha ?? "")
  )
    throw new Error(
      "Source run must be a successful completed mac-release.yml run from the private source repository",
    );
  const listed = await api(
    `/repos/${SOURCE_REPOSITORY}/actions/runs/${runID}/artifacts?per_page=100`,
    token,
  );
  const expected = [
    "macos-archive-arm64",
    "macos-archive-x64",
    "macos-26-evidence-arm64",
    "macos-26-evidence-x64",
  ];
  if (
    listed.total_count !== expected.length ||
    listed.artifacts?.length !== expected.length ||
    new Set(listed.artifacts.map((artifact) => artifact.name)).size !==
      expected.length ||
    listed.artifacts.some((artifact) => !expected.includes(artifact.name))
  )
    throw new Error(
      "Source run must contain exactly the four expected qualification artifacts",
    );
  await fs.mkdir(destination, { recursive: true });
  if ((await fs.readdir(destination)).length)
    throw new Error("Artifact destination must be empty");
  for (const name of expected) {
    const matches = listed.artifacts?.filter(
      (artifact) => artifact.name === name,
    );
    const artifact = matches?.[0];
    if (
      matches?.length !== 1 ||
      artifact.expired ||
      !Number.isSafeInteger(artifact.id) ||
      artifact.size_in_bytes > 2 * 1024 ** 3 ||
      !/^sha256:[a-f0-9]{64}$/.test(artifact.digest ?? "") ||
      (artifact.workflow_run?.head_sha &&
        artifact.workflow_run.head_sha !== run.head_sha)
    )
      throw new Error(`Missing or invalid source artifact: ${name}`);
    const response = await fetchImpl(
      `https://api.github.com/repos/${SOURCE_REPOSITORY}/actions/artifacts/${artifact.id}/zip`,
      {
        redirect: "manual",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
        },
      },
    );
    if (response.status !== 302)
      throw new Error(`Artifact download redirect missing: ${name}`);
    const location = new URL(response.headers.get("location"));
    if (location.protocol !== "https:")
      throw new Error("Artifact download must use HTTPS");
    // Signed storage URLs receive no private-repository credential.
    const download = await fetchImpl(location.href, { redirect: "error" });
    if (!download.ok || !download.body)
      throw new Error(`Artifact download failed: ${name}`);
    const zip = path.join(destination, `${name}.zip`);
    await pipeline(
      Readable.fromWeb(download.body),
      createWriteStream(zip, { flags: "wx" }),
    );
    if (`sha256:${await checksum(zip)}` !== artifact.digest)
      throw new Error(`GitHub artifact checksum mismatch: ${name}`);
    execFileSync(
      "python3",
      [
        "-c",
        "import pathlib,stat,sys,zipfile\nroot=pathlib.Path(sys.argv[2]); root.mkdir()\nwith zipfile.ZipFile(sys.argv[1]) as z:\n seen=set(); total=0\n for item in z.infolist():\n  p=pathlib.PurePosixPath(item.filename); total+=item.file_size\n  if p.is_absolute() or '..' in p.parts or item.filename in seen or stat.S_ISLNK(item.external_attr >> 16) or total > 4*1024**3: raise ValueError('Unsafe artifact ZIP member')\n  seen.add(item.filename)\n z.extractall(root)\n",
        zip,
        path.join(destination, name),
      ],
      { stdio: "inherit" },
    );
    await fs.rm(zip);
  }
  return { source_commit: run.head_sha, source_run_id: runID };
}
if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  try {
    const [mode, ...args] = process.argv.slice(2);
    let output;
    if (mode === "download" && args.length === 2)
      output = await downloadSourceArtifacts(args[0], path.resolve(args[1]));
    else if (mode === "assemble" && args.length === 2) {
      const manifest = await assembleArtifacts(
        path.resolve(args[0]),
        path.resolve(args[1]),
        process.env.SOURCE_COMMIT,
      );
      output = {
        version: manifest.version,
        source_commit: manifest.sourceCommit,
      };
    } else if (["stage", "promote"].includes(mode) && args.length === 1)
      output = await publishRelease(mode, path.resolve(args[0]));
    else
      throw new Error(
        "Usage: broker.mjs download RUN_ID DIR | assemble SOURCE DEST | stage DIR | promote DIR",
      );
    if (process.env.GITHUB_OUTPUT)
      await fs.appendFile(
        process.env.GITHUB_OUTPUT,
        Object.entries(output)
          .map(([key, value]) => `${key}=${value}`)
          .join("\n") + "\n",
      );
    console.log(JSON.stringify(output));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
