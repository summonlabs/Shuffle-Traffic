<#
.SYNOPSIS
  Clones the repository fresh and validates the real artifact from those bytes.

.DESCRIPTION
  The closure proof for a release: nothing is reused from the working tree.
  A clean clone of the pushed commit is configured, built, tested, installed to
  a clean prefix, and consumed by the independent downstream project through
  find_package. Paths are resolved from this script, so the script works from
  the clone it is validating.

  No timeout is passed to any build, test or install command: a hanging command
  is a defect to diagnose, not something to paper over.

.PARAMETER Remote
  Repository URL to clone. Defaults to the origin of the repository that
  contains this script.

.PARAMETER Ref
  Commit or tag to check out (for example v1.0.0). Defaults to HEAD.

.PARAMETER WorkDirectory
  Directory that receives the clone, the build trees and the install prefix.
  It is deleted first, so it must not contain anything else.

.EXAMPLE
  powershell -File scripts/validate-fresh-clone.ps1 -Ref v1.0.0
#>
[CmdletBinding()]
param(
  [string]$Remote = '',
  [string]$Ref = 'HEAD',
  [string]$WorkDirectory = ''
)

$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sourceRoot = Split-Path -Parent $scriptDirectory

function Write-Step([string]$Text) {
  Write-Host ""
  Write-Host "=== $Text ===" -ForegroundColor Cyan
}

$script:checkCount = 0
$script:passed = 0
$script:failures = @()
$script:resolvedCommit = ''

function Invoke-Checked([string]$Title, [scriptblock]$Body) {
  $script:checkCount++
  try {
    & $Body
    Write-Host ("[PASS] {0}" -f $Title) -ForegroundColor Green
    $script:passed++
  } catch {
    Write-Host ("[FAIL] {0} :: {1}" -f $Title, $_.Exception.Message) -ForegroundColor Red
    $script:failures += $Title
  }
}

# The clone must not live inside the source tree, or a build would see two
# copies of the same headers.
if ([string]::IsNullOrWhiteSpace($WorkDirectory)) {
  $WorkDirectory = Join-Path ([System.IO.Path]::GetTempPath()) 'shuffle-fabric-fresh-clone'
}
$work = [System.IO.Path]::GetFullPath($WorkDirectory)
$clone = Join-Path $work 'Shuffle-Fabric'

if ([string]::IsNullOrWhiteSpace($Remote)) {
  $Remote = (& git -C $sourceRoot remote get-url origin).Trim()
  if ([string]::IsNullOrWhiteSpace($Remote)) {
    throw 'no remote given and the source repository has no origin'
  }
}

Write-Host "fresh-clone validation"
Write-Host ("  source : {0}" -f $sourceRoot)
Write-Host ("  remote : {0}" -f $Remote)
Write-Host ("  ref    : {0}" -f $Ref)
Write-Host ("  work   : {0}" -f $work)

Invoke-Checked 'delete the work directory' {
  if (Test-Path -LiteralPath $work) {
    Remove-Item -LiteralPath $work -Recurse -Force
  }
  New-Item -ItemType Directory -Path $work -Force | Out-Null
}

Invoke-Checked 'clone the remote' {
  & git clone --quiet $Remote $clone
  if ($LASTEXITCODE -ne 0) { throw "git clone exited with $LASTEXITCODE" }
}

Invoke-Checked "check out the requested ref" {
  & git -C $clone checkout --quiet $Ref
  if ($LASTEXITCODE -ne 0) { throw "git checkout exited with $LASTEXITCODE" }
}

Invoke-Checked 'record the resolved commit' {
  $script:resolvedCommit = (& git -C $clone rev-parse HEAD).Trim()
  Write-Host ("  commit : {0}" -f $script:resolvedCommit)
}

$helper = Join-Path $clone 'scripts/msvc.ps1'
if (-not (Test-Path -LiteralPath $helper)) {
  throw "the clone has no scripts/msvc.ps1 at $helper"
}

$prefix = Join-Path $clone 'build/install-prefix'
$downstreamBuild = Join-Path $clone 'build/downstream'

Invoke-Checked 'configure Release in the clone' {
  & $helper -WorkDir $clone -Command 'cmake -S . -B build/release -G Ninja -DCMAKE_BUILD_TYPE=Release'
}

Invoke-Checked 'build Release in the clone' {
  & $helper -WorkDir $clone -Command 'cmake --build build/release'
}

Invoke-Checked 'run the test suite in the clone' {
  & $helper -WorkDir $clone -Command 'ctest --test-dir build/release --output-on-failure'
}

Invoke-Checked 'install to a clean prefix' {
  & $helper -WorkDir $clone -Command 'cmake --install build/release --prefix build/install-prefix'
  $config = Join-Path $prefix 'lib/cmake/ShuffleFabric/ShuffleFabricConfig.cmake'
  if (-not (Test-Path -LiteralPath $config)) {
    throw "the installed package has no ShuffleFabricConfig.cmake at $config"
  }
}

Invoke-Checked 'configure the downstream consumer against the installed prefix' {
  $argument = '-DCMAKE_PREFIX_PATH="' + $prefix + '"'
  & $helper -WorkDir $clone -Command ('cmake -S downstream/shuffle-consumer-demo -B build/downstream -G Ninja -DCMAKE_BUILD_TYPE=Release ' + $argument)
}

Invoke-Checked 'build the downstream consumer' {
  & $helper -WorkDir $clone -Command 'cmake --build build/downstream'
}

Invoke-Checked 'run the downstream consumer' {
  # The downstream project chooses its own target name, so the executable is
  # discovered rather than assumed.
  $demo = $null
  foreach ($candidate in Get-ChildItem -LiteralPath $downstreamBuild -Filter '*.exe' -ErrorAction SilentlyContinue) {
    if ($candidate.Name -match 'consumer|demo') {
      $demo = $candidate.FullName
      break
    }
  }
  if ($null -eq $demo) {
    throw "the downstream demo binary was not produced in $downstreamBuild"
  }
  & $demo
  if ($LASTEXITCODE -ne 0) { throw "the demo exited with $LASTEXITCODE" }
}

Write-Step 'summary'
Write-Host ("  commit        : {0}" -f $script:resolvedCommit)
Write-Host ("  checks passed : {0}/{1}" -f $script:passed, $script:checkCount)
if ($script:failures.Count -gt 0) {
  foreach ($failure in $script:failures) {
    Write-Host ("  failed        : {0}" -f $failure) -ForegroundColor Red
  }
  Write-Host "fresh-clone validation: FAIL" -ForegroundColor Red
  exit 1
}
Write-Host "fresh-clone validation: PASS" -ForegroundColor Green
exit 0
