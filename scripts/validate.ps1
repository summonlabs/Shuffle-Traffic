<#
.SYNOPSIS
  Full validation entry point for ShuffleFabric.

.DESCRIPTION
  Runs, in order and failing fast:

    1. Release       : configure + build + ctest in build/release
    2. Debug         : configure + build + ctest in build/debug
    3. AddressSanitizer: configure + build + ctest in build/asan
                       (-DSHUFFLE_FABRIC_SANITIZE_ADDRESS=ON)
    4. install       : scripts/validate-install.ps1 (installed package plus the
                       independent downstream consumer)

  Everything runs through scripts/msvc.ps1, so the script works from any working
  directory, always uses the MSVC toolchain, and ctest finds the AddressSanitizer
  runtime that a sanitizer build links against -- a sanitizer test binary fails
  with STATUS_DLL_NOT_FOUND (0xC0000135) outside the developer environment.

  Tools and examples are switched off: this script validates the library and its
  test suite, and the packaging path is validated separately by
  validate-install.ps1.

  No timeout is applied anywhere: not to ctest, not to a test binary and not to
  a build. A hanging test is a defect to diagnose, and neither this script nor
  the project registers a CTest TIMEOUT property.

  The script is re-runnable: build trees are never deleted, cmake is simply run
  again on them, and ctest re-runs whatever tests the tree currently registers,
  so a fixed test list is never assumed.

.NOTES
  Exit code 0 means every step passed. Exit code 1 means a step failed; the
  captured output of the failing step is printed before the script exits.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# A native command that writes to stderr is not an exception; exit codes are
# what this script reacts to.
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

# ---------------------------------------------------------------------------
# Layout. Everything is resolved relative to this script, never relative to the
# caller's working directory.
# ---------------------------------------------------------------------------
$RepoRoot = Split-Path -Parent $PSScriptRoot
$MsvcScript = Join-Path $PSScriptRoot 'msvc.ps1'
$ValidateInstallScript = Join-Path $PSScriptRoot 'validate-install.ps1'
$BuildRoot = Join-Path $RepoRoot 'build'
$LogDir = Join-Path $BuildRoot 'validate-logs'

$Configurations = @(
  [pscustomobject]@{ Label = 'release'; BuildType = 'Release'; Directory = (Join-Path $BuildRoot 'release'); Sanitizer = $false },
  [pscustomobject]@{ Label = 'debug'; BuildType = 'Debug'; Directory = (Join-Path $BuildRoot 'debug'); Sanitizer = $false },
  [pscustomobject]@{ Label = 'asan'; BuildType = 'Debug'; Directory = (Join-Path $BuildRoot 'asan'); Sanitizer = $true }
)

$script:Results = New-Object System.Collections.Generic.List[object]
$script:CurrentLog = $null

function Add-Result {
  param([string]$Configuration, [string]$Step, [string]$Status, [string]$Detail = '', [double]$Seconds = 0)
  $script:Results.Add([pscustomobject]@{
    Configuration = $Configuration
    Step = $Step
    Status = $Status
    Detail = $Detail
    Seconds = $Seconds
  })
}

function Quote-Arg {
  param([string]$Value)
  return '"' + $Value + '"'
}

# Runs one compiler-environment command through scripts/msvc.ps1, tees its
# stdout into a log, and fails the script when the command exits non-zero.
function Invoke-MsvcStep {
  param(
    [Parameter(Mandatory = $true)][string]$Configuration,
    [Parameter(Mandatory = $true)][string]$Step,
    [Parameter(Mandatory = $true)][string]$Command,
    [Parameter(Mandatory = $true)][string]$LogFile,
    [string]$Detail = ''
  )

  Write-Host ''
  Write-Host ('>>> ' + $Configuration + ' / ' + $Step) -ForegroundColor Cyan
  Write-Host ('    ' + $Command) -ForegroundColor DarkGray
  $script:CurrentLog = $LogFile
  $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
  & $MsvcScript -Command $Command -WorkDir $RepoRoot | Tee-Object -FilePath $LogFile
  $stopwatch.Stop()
  $exitCode = $LASTEXITCODE
  $seconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
  if ($exitCode -ne 0) {
    Add-Result -Configuration $Configuration -Step $Step -Status 'FAIL' -Detail ('exit code ' + $exitCode) -Seconds $seconds
    throw ($Configuration + ' / ' + $Step + ' failed with exit code ' + $exitCode)
  }
  Add-Result -Configuration $Configuration -Step $Step -Status 'PASS' -Detail $Detail -Seconds $seconds
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

# Reads the ctest verdict out of a ctest log: the tests that ran, the pass ratio
# and the failure list, whatever the tree currently registers.
function Get-CtestDetail {
  param([string]$LogPath)

  if (-not (Test-Path -LiteralPath $LogPath -PathType Leaf)) { return 'no ctest output' }
  $lines = @(Get-Content -LiteralPath $LogPath)
  $names = New-Object System.Collections.Generic.List[string]
  $summary = ''
  foreach ($line in $lines) {
    $named = [regex]::Match($line, 'Test\s+#\d+:\s+(\S+)')
    if ($named.Success) { $names.Add($named.Groups[1].Value) }
    $verdict = [regex]::Match($line, '(\d+)% tests passed, (\d+) tests failed out of (\d+)')
    if ($verdict.Success) {
      $total = [int]$verdict.Groups[3].Value
      $failed = [int]$verdict.Groups[2].Value
      $summary = (($total - $failed).ToString() + '/' + $total.ToString() + ' tests passed')
    }
  }
  if (-not $summary) { return ('test output present, no ctest verdict found (' + $names.Count.ToString() + ' tests seen)') }
  $listed = ''
  if ($names.Count -gt 0) {
    if ($names.Count -le 10) {
      $listed = ' [' + ($names -join ', ') + ']'
    } else {
      $listed = ' [' + (($names[0..8]) -join ', ') + ', +' + ($names.Count - 9).ToString() + ' more]'
    }
  }
  return ($summary + $listed)
}

function Write-Summary {
  param([bool]$Succeeded, [string]$FailedStep)

  Write-Host ''
  Write-Host '============================================================'
  Write-Host 'validate.ps1 summary'
  Write-Host '------------------------------------------------------------'
  Write-Host ('repository : ' + $RepoRoot)
  Write-Host ('logs       : ' + $LogDir)
  Write-Host '------------------------------------------------------------'
  $passCount = 0
  $checkCount = 0
  foreach ($result in $script:Results) {
    $color = 'Red'
    if ($result.Status -eq 'PASS') { $color = 'Green' }
    if ($result.Status -eq 'PASS' -or $result.Status -eq 'FAIL') { $checkCount = $checkCount + 1 }
    if ($result.Status -eq 'PASS') { $passCount = $passCount + 1 }
    $line = '{0,-8} {1,-14} {2,-5} {3,7}s  {4}' -f $result.Configuration, $result.Step, $result.Status, $result.Seconds, $result.Detail
    Write-Host $line -ForegroundColor $color
  }
  Write-Host '------------------------------------------------------------'
  if ($Succeeded) {
    Write-Host ('RESULT: PASS (' + $passCount.ToString() + '/' + $checkCount.ToString() + ' checks)') -ForegroundColor Green
  } else {
    Write-Host ('RESULT: FAIL (' + $passCount.ToString() + '/' + $checkCount.ToString() + ' checks) -- ' + $FailedStep) -ForegroundColor Red
  }
  Write-Host '============================================================'
}

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
foreach ($required in @($MsvcScript, $ValidateInstallScript, (Join-Path $RepoRoot 'CMakeLists.txt'))) {
  if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
    throw ('required file is missing: ' + $required)
  }
}
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$failedStep = ''
$succeeded = $false

try {
  Write-Host 'ShuffleFabric validation' -ForegroundColor White
  Write-Host ('compiler environment: ' + $MsvcScript)
  Write-Host ('configurations      : ' + (($Configurations | ForEach-Object { $_.Label + '(' + $_.BuildType + ')' }) -join ', '))

  # -- 1..3: every configuration in its own build tree ----------------------
  foreach ($configuration in $Configurations) {
    Write-Host ''
    Write-Host ('=== ' + $configuration.Label + ' (' + $configuration.BuildType + ', ' + $configuration.Directory + ') ===') -ForegroundColor White

    $configureCommand = 'cmake -S ' + (Quote-Arg $RepoRoot) + ' -B ' + (Quote-Arg $configuration.Directory) + ' -G Ninja -DCMAKE_BUILD_TYPE=' + $configuration.BuildType + ' -DSHUFFLE_FABRIC_BUILD_TOOLS=OFF -DSHUFFLE_FABRIC_BUILD_EXAMPLES=OFF'
    if ($configuration.Sanitizer) {
      $configureCommand = $configureCommand + ' -DSHUFFLE_FABRIC_SANITIZE_ADDRESS=ON'
    }

    $steps = @(
      @{ Step = 'configure'; Command = $configureCommand; LogFile = (Join-Path $LogDir ($configuration.Label + '-configure.log')); Detail = '' },
      @{ Step = 'build'; Command = ('cmake --build ' + (Quote-Arg $configuration.Directory)); LogFile = (Join-Path $LogDir ($configuration.Label + '-build.log')); Detail = '' }
    )
    foreach ($item in $steps) {
      $arguments = @{ Configuration = $configuration.Label; Step = $item.Step; Command = $item.Command; LogFile = $item.LogFile; Detail = $item.Detail }
      Invoke-MsvcStep @arguments
    }

    # ctest discovers whatever the tree registers; no test list is assumed and
    # no timeout is passed.
    $ctestLog = Join-Path $LogDir ($configuration.Label + '-ctest.log')
    $ctestArguments = @{
      Configuration = $configuration.Label
      Step = 'ctest'
      Command = ('ctest --test-dir ' + (Quote-Arg $configuration.Directory) + ' --output-on-failure')
      LogFile = $ctestLog
      Detail = ''
    }
    Invoke-MsvcStep @ctestArguments
    $verdict = Get-CtestDetail -LogPath $ctestLog
    $last = $script:Results[$script:Results.Count - 1]
    $script:Results[$script:Results.Count - 1] = [pscustomobject]@{
      Configuration = $last.Configuration
      Step = $last.Step
      Status = $last.Status
      Detail = $verdict
      Seconds = $last.Seconds
    }
  }

  # -- 4: the installed package and the independent downstream consumer -----
  Write-Host ''
  Write-Host '=== install / downstream consumer ===' -ForegroundColor White
  $installLog = Join-Path $LogDir 'install.log'
  $script:CurrentLog = $installLog
  Write-Host '>>> install / validate-install.ps1' -ForegroundColor Cyan
  Write-Host ('    powershell -File ' + $ValidateInstallScript) -ForegroundColor DarkGray
  $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
  & $ValidateInstallScript 6>&1 | Tee-Object -FilePath $installLog
  $stopwatch.Stop()
  $installExitCode = $LASTEXITCODE
  $installSeconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
  if ($installExitCode -ne 0) {
    Add-Result -Configuration 'install' -Step 'validate-install' -Status 'FAIL' -Detail ('exit code ' + $installExitCode) -Seconds $installSeconds
    throw ('install / validate-install.ps1 failed with exit code ' + $installExitCode)
  }
  Add-Result -Configuration 'install' -Step 'validate-install' -Status 'PASS' -Detail 'installed package + downstream demo' -Seconds $installSeconds

  $succeeded = $true
} catch {
  $failedStep = $_.Exception.Message
  Write-Host ''
  Write-Host ('FAILED: ' + $failedStep) -ForegroundColor Red
  Write-CapturedFailure -LogPath $script:CurrentLog
  Write-Host ('full logs: ' + $LogDir) -ForegroundColor Red
}

Write-Summary -Succeeded $succeeded -FailedStep $failedStep

if ($succeeded) { exit 0 }
exit 1
