<#
.SYNOPSIS
  Validates the INSTALLED ShuffleFabric package against an independent consumer.

.DESCRIPTION
  Re-runnable end-to-end check of the packaging contract:

    1. configure + build Release into build/install-check (no tools, no
       examples, no tests);
    2. install into a CLEAN prefix -- build/install-prefix is deleted first;
    3. assert the prefix carries the public headers, the static library and the
       ShuffleFabricConfig.cmake / ShuffleFabricConfigVersion.cmake /
       ShuffleFabricTargets.cmake package files, and that the targets file
       defines the documented imported target ShuffleFabric::fabric;
    4. configure + build downstream/shuffle-consumer-demo -- a separate CMake
       project -- in its own build directory, resolving the package only
       through CMAKE_PREFIX_PATH;
    5. run the demo and require exit code 0;
    6. print a PASS/FAIL summary.

  Every compiler-environment command runs through scripts/msvc.ps1, so the
  script works from any working directory. No build, install or test command is
  given a timeout: a hang is a defect to diagnose, not something to paper over.

.NOTES
  Exit code 0 means every step passed. Exit code 1 means a step failed; the
  captured output of the failing step is printed before the script exits.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# A native command that writes to stderr is not an exception; the demo reports
# failures on stderr and the exit code is what this script reacts to.
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

# ---------------------------------------------------------------------------
# Layout. Everything is resolved relative to this script, never relative to the
# caller's working directory, and nothing here is machine-specific.
# ---------------------------------------------------------------------------
$RepoRoot = Split-Path -Parent $PSScriptRoot
$MsvcScript = Join-Path $PSScriptRoot 'msvc.ps1'
$BuildRoot = Join-Path $RepoRoot 'build'
$InstallCheckBuild = Join-Path $BuildRoot 'install-check'
$InstallPrefix = Join-Path $BuildRoot 'install-prefix'
$DownstreamSource = Join-Path (Join-Path $RepoRoot 'downstream') 'shuffle-consumer-demo'
$DownstreamBuild = Join-Path $BuildRoot 'downstream-demo'
$LogDir = Join-Path $BuildRoot 'validate-logs'
$DemoBaseName = 'shuffle_consumer_demo'
$PackageDirRelative = 'lib/cmake/ShuffleFabric'

$script:Results = New-Object System.Collections.Generic.List[object]
$script:CurrentLog = $null

function Add-Result {
  param([string]$Name, [string]$Status, [string]$Detail = '')
  $script:Results.Add([pscustomobject]@{ Name = $Name; Status = $Status; Detail = $Detail })
}

function Quote-Arg {
  param([string]$Value)
  return '"' + $Value + '"'
}

# Runs one compiler-environment command through scripts/msvc.ps1, tees its
# stdout to a log and fails the script when the command exits non-zero.
function Invoke-MsvcCommand {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$Command,
    [Parameter(Mandatory = $true)][string]$LogFile,
    [string]$Detail = ''
  )

  Write-Host ''
  Write-Host ('>>> ' + $Name) -ForegroundColor Cyan
  Write-Host ('    ' + $Command) -ForegroundColor DarkGray
  $script:CurrentLog = $LogFile
  & $MsvcScript -Command $Command -WorkDir $RepoRoot | Tee-Object -FilePath $LogFile
  $exitCode = $LASTEXITCODE
  if ($exitCode -ne 0) {
    Add-Result -Name $Name -Status 'FAIL' -Detail ('exit code ' + $exitCode)
    throw ($Name + ' failed with exit code ' + $exitCode)
  }
  Add-Result -Name $Name -Status 'PASS' -Detail $Detail
}

function Write-CapturedFailure {
  param([string]$LogPath)
  if (-not $LogPath) { return }
  if (-not (Test-Path -LiteralPath $LogPath -PathType Leaf)) {
    Write-Host '(no captured output for this step)' -ForegroundColor Red
    return
  }
  $lines = @(Get-Content -LiteralPath $LogPath)
  Write-Host ('---- captured output: ' + $LogPath + ' (' + $lines.Count + ' lines) ----') -ForegroundColor Red
  if ($lines.Count -le 200) {
    foreach ($line in $lines) { Write-Host $line }
  } else {
    Write-Host ('(showing the last 200 of ' + $lines.Count + ' lines)')
    foreach ($line in $lines[($lines.Count - 200)..($lines.Count - 1)]) { Write-Host $line }
  }
  Write-Host '---- end captured output ----' -ForegroundColor Red
}

function Find-DemoExecutable {
  param([string]$BuildDir, [string]$BaseName)

  $candidates = @(
    (Join-Path $BuildDir ($BaseName + '.exe')),
    (Join-Path $BuildDir $BaseName),
    (Join-Path $BuildDir (Join-Path 'Release' ($BaseName + '.exe'))),
    (Join-Path $BuildDir (Join-Path 'bin' ($BaseName + '.exe'))),
    (Join-Path $BuildDir (Join-Path 'bin' (Join-Path 'Release' ($BaseName + '.exe'))))
  )
  foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate -PathType Leaf) { return $candidate }
  }
  if (Test-Path -LiteralPath $BuildDir) {
    $found = Get-ChildItem -LiteralPath $BuildDir -Recurse -File -Filter ($BaseName + '.exe') | Select-Object -First 1
    if ($found) { return $found.FullName }
  }
  return $null
}

function Write-Summary {
  param([bool]$Succeeded, [string]$FailedStep)

  Write-Host ''
  Write-Host '============================================================'
  Write-Host 'validate-install.ps1 summary'
  Write-Host '------------------------------------------------------------'
  Write-Host ('repository     : ' + $RepoRoot)
  Write-Host ('install prefix : ' + $InstallPrefix)
  Write-Host ('downstream     : ' + $DownstreamSource)
  Write-Host '------------------------------------------------------------'
  $passCount = 0
  $checkCount = 0
  foreach ($result in $script:Results) {
    $color = 'Yellow'
    if ($result.Status -eq 'PASS') {
      $color = 'Green'
      $passCount = $passCount + 1
      $checkCount = $checkCount + 1
    } elseif ($result.Status -eq 'FAIL') {
      $color = 'Red'
      $checkCount = $checkCount + 1
    }
    $line = '[{0}] {1}' -f $result.Status, $result.Name
    if ($result.Detail) { $line = $line + ' -- ' + $result.Detail }
    Write-Host $line -ForegroundColor $color
  }
  Write-Host '------------------------------------------------------------'
  if ($Succeeded) {
    Write-Host ('RESULT: PASS (' + $passCount + '/' + $checkCount + ' checks)') -ForegroundColor Green
  } else {
    Write-Host ('RESULT: FAIL (' + $passCount + '/' + $checkCount + ' checks) -- ' + $FailedStep) -ForegroundColor Red
  }
  Write-Host '============================================================'
}

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
foreach ($required in @($MsvcScript, (Join-Path $RepoRoot 'CMakeLists.txt'), (Join-Path $DownstreamSource 'CMakeLists.txt'), (Join-Path $DownstreamSource 'main.cpp'))) {
  if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
    throw ('required file is missing: ' + $required)
  }
}

New-Item -ItemType Directory -Path $BuildRoot -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$failedStep = ''
$passed = $false

try {
  Write-Host 'ShuffleFabric install validation' -ForegroundColor White
  Write-Host ('compiler environment: ' + $MsvcScript)

  # -- 1. configure + build the library in its own Release build tree --------
  $configureCommand = 'cmake -S ' + (Quote-Arg $RepoRoot) + ' -B ' + (Quote-Arg $InstallCheckBuild) + ' -G Ninja -DCMAKE_BUILD_TYPE=Release -DSHUFFLE_FABRIC_BUILD_TESTS=OFF -DSHUFFLE_FABRIC_BUILD_TOOLS=OFF -DSHUFFLE_FABRIC_BUILD_EXAMPLES=OFF'
  $stepOne = @{ Name = 'configure Release (build/install-check)'; Command = $configureCommand; LogFile = (Join-Path $LogDir '01-configure.log') }
  Invoke-MsvcCommand @stepOne

  $stepTwo = @{ Name = 'build Release (build/install-check)'; Command = ('cmake --build ' + (Quote-Arg $InstallCheckBuild)); LogFile = (Join-Path $LogDir '02-build.log') }
  Invoke-MsvcCommand @stepTwo

  # -- 2. install into a clean prefix ---------------------------------------
  $prefixExisted = Test-Path -LiteralPath $InstallPrefix
  if ($prefixExisted) {
    Remove-Item -LiteralPath $InstallPrefix -Recurse -Force
  }
  $installLog = Join-Path $LogDir '03-install.log'
  $stepThree = @{
    Name = 'install to clean prefix'
    Command = ('cmake --install ' + (Quote-Arg $InstallCheckBuild) + ' --prefix ' + (Quote-Arg $InstallPrefix))
    LogFile = $installLog
    Detail = ('prefix deleted first, existed before: ' + $prefixExisted.ToString().ToLowerInvariant())
  }
  Invoke-MsvcCommand @stepThree

  # -- 3. assert the installed package --------------------------------------
  $missing = New-Object System.Collections.Generic.List[string]
  $sourceHeaderDir = Join-Path (Join-Path (Join-Path $RepoRoot 'include') 'shuffle') 'fabric'
  $sourceHeaders = @(Get-ChildItem -LiteralPath $sourceHeaderDir -File -Filter '*.hpp' | ForEach-Object { $_.Name })
  if ($sourceHeaders.Count -eq 0) {
    throw ('no public headers found in ' + $sourceHeaderDir)
  }
  $headerRelativeDir = Join-Path (Join-Path 'include' 'shuffle') 'fabric'
  foreach ($header in $sourceHeaders) {
    $installedHeader = Join-Path $InstallPrefix (Join-Path $headerRelativeDir $header)
    if (-not (Test-Path -LiteralPath $installedHeader -PathType Leaf)) { $missing.Add($installedHeader) }
  }

  $libraryCandidates = @('shuffle_fabric.lib', 'libshuffle_fabric.a', 'shuffle_fabric.a', 'shuffle_fabric.dll')
  $installedLibrary = ''
  foreach ($libraryName in $libraryCandidates) {
    $candidate = Join-Path $InstallPrefix (Join-Path 'lib' $libraryName)
    if (Test-Path -LiteralPath $candidate -PathType Leaf) { $installedLibrary = $candidate; break }
  }
  if (-not $installedLibrary) {
    $missing.Add((Join-Path $InstallPrefix 'lib/<shuffle_fabric library>'))
  }

  $packageDir = Join-Path $InstallPrefix $PackageDirRelative
  $packageFiles = @('ShuffleFabricConfig.cmake', 'ShuffleFabricConfigVersion.cmake', 'ShuffleFabricTargets.cmake')
  foreach ($packageFile in $packageFiles) {
    $installedPackageFile = Join-Path $InstallPrefix (Join-Path $PackageDirRelative $packageFile)
    if (-not (Test-Path -LiteralPath $installedPackageFile -PathType Leaf)) { $missing.Add($installedPackageFile) }
  }

  if ($missing.Count -gt 0) {
    Add-Result -Name 'installed package contains the expected files' -Status 'FAIL' -Detail ($missing.Count.ToString() + ' missing')
    foreach ($item in $missing) { Write-Host ('    missing: ' + $item) -ForegroundColor Red }
    throw ('the installed prefix is incomplete (' + $missing.Count + ' expected file(s) missing)')
  }
  $installedFileCount = 0
  if (Test-Path -LiteralPath $installLog -PathType Leaf) {
    $installedFileCount = @(Select-String -LiteralPath $installLog -Pattern '^-- Installing: ').Count
  }
  Add-Result -Name 'installed package contains the expected files' -Status 'PASS' -Detail ($installedFileCount.ToString() + ' files installed: ' + $sourceHeaders.Count.ToString() + ' headers, ' + (Split-Path -Leaf $installedLibrary) + ', 3 cmake files')

  # The documented imported target name. A package that exports the library
  # under its build name instead would still link through a consumer-side alias,
  # so the exact exported name is asserted here instead of being inferred from a
  # successful build.
  $targetsFile = Join-Path $packageDir 'ShuffleFabricTargets.cmake'
  $targetsText = ''
  if (Test-Path -LiteralPath $targetsFile -PathType Leaf) {
    $targetsText = Get-Content -LiteralPath $targetsFile -Raw
  }
  if ($targetsText -match 'add_library\(ShuffleFabric::fabric ') {
    Add-Result -Name 'ShuffleFabricTargets.cmake defines ShuffleFabric::fabric' -Status 'PASS' -Detail ''
  } else {
    $exported = @([regex]::Matches($targetsText, 'add_library\((ShuffleFabric::[A-Za-z0-9_]+) ') | ForEach-Object { $_.Groups[1].Value })
    $found = 'nothing'
    if ($exported.Count -gt 0) { $found = ($exported -join ', ') }
    Add-Result -Name 'ShuffleFabricTargets.cmake defines ShuffleFabric::fabric' -Status 'FAIL' -Detail ('exported targets: ' + $found)
    throw ('the installed targets file does not define ShuffleFabric::fabric (found: ' + $found + ')')
  }

  Write-Host ''
  Write-Host ('installed files: ' + $installedFileCount) -ForegroundColor DarkGray
  Write-Host ('installed headers: ' + $sourceHeaders.Count) -ForegroundColor DarkGray
  Write-Host ('installed library: ' + $installedLibrary) -ForegroundColor DarkGray
  Write-Host 'installed package files:' -ForegroundColor DarkGray
  foreach ($entry in (Get-ChildItem -LiteralPath $packageDir -File | Sort-Object -Property Name)) {
    Write-Host ('    ' + $PackageDirRelative + '/' + $entry.Name) -ForegroundColor DarkGray
  }

  # -- 4. configure + build the independent downstream project --------------
  if (Test-Path -LiteralPath $DownstreamBuild) {
    Remove-Item -LiteralPath $DownstreamBuild -Recurse -Force
  }
  $downstreamConfigureCommand = 'cmake -S ' + (Quote-Arg $DownstreamSource) + ' -B ' + (Quote-Arg $DownstreamBuild) + ' -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=' + (Quote-Arg $InstallPrefix)
  $stepFour = @{ Name = 'configure downstream demo against prefix'; Command = $downstreamConfigureCommand; LogFile = (Join-Path $LogDir '04-downstream-configure.log') }
  Invoke-MsvcCommand @stepFour

  $stepFive = @{ Name = 'build downstream demo'; Command = ('cmake --build ' + (Quote-Arg $DownstreamBuild)); LogFile = (Join-Path $LogDir '05-downstream-build.log') }
  Invoke-MsvcCommand @stepFive

  # The cache proves which package the demo resolved: the installed prefix, and
  # not the source tree.
  $cachePath = Join-Path $DownstreamBuild 'CMakeCache.txt'
  if (-not (Test-Path -LiteralPath $cachePath -PathType Leaf)) {
    Add-Result -Name 'downstream resolved the installed package' -Status 'FAIL' -Detail 'no CMakeCache.txt'
    throw ('the downstream build produced no CMakeCache.txt at ' + $cachePath)
  }
  $resolvedDir = ''
  foreach ($line in (Get-Content -LiteralPath $cachePath)) {
    if ($line -like 'ShuffleFabric_DIR:PATH=*') {
      $resolvedDir = $line.Substring('ShuffleFabric_DIR:PATH='.Length)
      break
    }
  }
  $expectedDir = (Join-Path $InstallPrefix $PackageDirRelative).Replace('\', '/')
  if (-not $resolvedDir) {
    Add-Result -Name 'downstream resolved the installed package' -Status 'FAIL' -Detail 'ShuffleFabric_DIR is not cached'
    throw 'find_package did not record ShuffleFabric_DIR'
  }
  if ($resolvedDir.Replace('\', '/').TrimEnd('/') -ne $expectedDir.TrimEnd('/')) {
    Add-Result -Name 'downstream resolved the installed package' -Status 'FAIL' -Detail ('resolved ' + $resolvedDir)
    throw ('the downstream project resolved ' + $resolvedDir + ' instead of ' + $expectedDir)
  }
  Add-Result -Name 'downstream resolved the installed package' -Status 'PASS' -Detail $resolvedDir

  # -- 5. run the demo ------------------------------------------------------
  $demoExecutable = Find-DemoExecutable -BuildDir $DownstreamBuild -BaseName $DemoBaseName
  if (-not $demoExecutable) {
    Add-Result -Name 'run downstream demo' -Status 'FAIL' -Detail 'executable not found'
    throw ('the demo executable was not produced under ' + $DownstreamBuild)
  }

  Write-Host ''
  Write-Host ('>>> run downstream demo (' + $demoExecutable + ')') -ForegroundColor Cyan
  $demoLog = Join-Path $LogDir '06-demo-run.log'
  $script:CurrentLog = $demoLog
  $demoOutput = @(& $demoExecutable | Tee-Object -FilePath $demoLog)
  $demoExitCode = $LASTEXITCODE
  Write-Host ''
  if ($demoExitCode -ne 0) {
    Add-Result -Name 'run downstream demo' -Status 'FAIL' -Detail ('exit code ' + $demoExitCode)
    throw ('the downstream demo exited with code ' + $demoExitCode)
  }
  Add-Result -Name 'run downstream demo' -Status 'PASS' -Detail ('exit code 0, ' + $demoOutput.Count + ' lines of output')

  # The demo prints a deterministic summary; these two lines carry the proof of
  # the closure invariant and must be present even though the exit code is 0.
  $demoText = $demoOutput -join [Environment]::NewLine
  if ($demoText -notmatch 'demo_result=PASS') {
    Add-Result -Name 'demo reported PASS' -Status 'FAIL' -Detail 'demo_result=PASS is absent'
    throw 'the demo did not report demo_result=PASS'
  }
  if ($demoText -notmatch 'accounting_closes=1') {
    Add-Result -Name 'demo reported PASS' -Status 'FAIL' -Detail 'accounting_closes=1 is absent'
    throw 'the demo did not report accounting_closes=1'
  }
  Add-Result -Name 'demo reported PASS' -Status 'PASS' -Detail 'accounting_closes=1'

  Write-Host 'downstream demo output:' -ForegroundColor DarkGray
  foreach ($line in $demoOutput) { Write-Host ('    ' + $line) -ForegroundColor DarkGray }

  $passed = $true
} catch {
  $failedStep = $_.Exception.Message
  Write-Host ''
  Write-Host ('FAILED: ' + $failedStep) -ForegroundColor Red
  Write-CapturedFailure -LogPath $script:CurrentLog
  Write-Host ('full logs: ' + $LogDir) -ForegroundColor Red
}

Write-Summary -Succeeded $passed -FailedStep $failedStep

if ($passed) { exit 0 }
exit 1
