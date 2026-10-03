# Prepare the repository's pinned native voice inputs on a Windows CI runner.
$ErrorActionPreference = "Stop"
$manifest = Get-Content -Raw (Join-Path $PSScriptRoot "voice-cygwin-snapshot.json") | ConvertFrom-Json
$directory = Join-Path $env:RUNNER_TEMP "voice-cygwin-snapshot"
New-Item -ItemType Directory -Path $directory | Out-Null
$archive = Join-Path $directory $manifest.archive.name
# The public upstream release carries this exact archive and its matching sources.
$url = "https://github.com/openai/codex/releases/download/$($manifest.archive.tag)/$($manifest.archive.name)"
Invoke-WebRequest -Uri $url -OutFile $archive
if ((Get-Item $archive).Length -ne $manifest.archive.bytes -or
        (Get-FileHash $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $manifest.archive.sha256) {
    throw "Pinned Cygwin build archive mismatch"
}
$hostArch = $env:PROCESSOR_ARCHITEW6432
if (-not $hostArch) { $hostArch = $env:PROCESSOR_ARCHITECTURE }
$target = switch ($hostArch) {
    "AMD64" { "x86_64-pc-windows-msvc" }
    "ARM64" { "aarch64-pc-windows-msvc" }
    default { throw "Unsupported Windows host architecture: $hostArch" }
}
if (-not $env:SystemRoot) { throw "Windows SystemRoot is required for native audio actions" }
& (Join-Path $PSScriptRoot "setup-voice-windows.ps1") -Target $target -SnapshotArchive $archive
"VOICE_WINDOWS_SYSTEM_ROOT=$env:SystemRoot" | Out-File $env:GITHUB_ENV -Encoding utf8 -Append
"VOICE_WINDOWS_HOST_ARCH=$hostArch" | Out-File $env:GITHUB_ENV -Encoding utf8 -Append
