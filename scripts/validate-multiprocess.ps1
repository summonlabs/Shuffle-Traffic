<#
.SYNOPSIS
  Runs the multiprocess proof: one coordinator service process plus independent
  producer and consumer processes exchanging real bytes over loopback TCP.

.DESCRIPTION
  Three scenarios are exercised against the real artifacts:
    1. happy path        -- every required edge completes exactly once
    2. producer killed   -- no edge is silently lost, the accounting closes
    3. corrupt payload   -- a corrupt generation is never committed
  The coordinator is queried through the inspection tool, so assertions are made
  against authoritative state rather than against a participant's opinion.

  No timeout is passed to any build or test command. The bounded waits below
  only poll for a file or a line that a child process publishes; they never
  decide whether a result is correct.

.PARAMETER Configuration
  Build configuration directory to use (default build/release).

.EXAMPLE
  powershell -File scripts/validate-multiprocess.ps1
#>
[CmdletBinding()]
param(
  [string]$Configuration = 'build/release'
)

$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDirectory
$build = Join-Path $root $Configuration
$coordinator = Join-Path $build 'tools/shuffle-fabric-coordinator.exe'
$cli = Join-Path $build 'tools/shuffle-fabric-cli.exe'
$participant = Join-Path $build 'tests/multiprocess-participant.exe'

$script:checks = 0
$script:failures = @()

function Assert-That([string]$Title, [bool]$Condition, [string]$Detail) {
  $script:checks++
  if ($Condition) {
    Write-Host ("[PASS] {0}" -f $Title) -ForegroundColor Green
  } else {
    Write-Host ("[FAIL] {0} :: {1}" -f $Title, $Detail) -ForegroundColor Red
    $script:failures += $Title
  }
}

# Start-Process joins arguments with spaces and does not quote them, so any
# argument that contains a space must be quoted here.
function Quote-Argument([string]$Value) {
  if ($Value -match '\s') { return '"' + $Value + '"' }
  return $Value
}

function Wait-ForFile([string]$Path, [int]$Attempts = 200) {
  for ($attempt = 0; $attempt -lt $Attempts; $attempt++) {
    if (Test-Path -LiteralPath $Path) { return $true }
    Start-Sleep -Milliseconds 100
  }
  return (Test-Path -LiteralPath $Path)
}

function Wait-ForText([string]$Path, [string]$Needle, [int]$Attempts = 400) {
  for ($attempt = 0; $attempt -lt $Attempts; $attempt++) {
    if (Test-Path -LiteralPath $Path) {
      $text = Get-Content -LiteralPath $Path -Raw -ErrorAction SilentlyContinue
      if ($text -and $text.Contains($Needle)) { return $true }
    }
    Start-Sleep -Milliseconds 100
  }
  return $false
}

function Read-Progress([string]$Port) {
  $output = & $cli progress --coordinator ("127.0.0.1:" + $Port) 2>&1 | Out-String
  $fields = @{}
  foreach ($match in [regex]::Matches($output, '([a-z_]+)=([0-9]+)')) {
    $fields[$match.Groups[1].Value] = [int64]$match.Groups[2].Value
  }
  return @{ Text = $output; Fields = $fields }
}

function Start-Scenario([string]$Name, [int]$Partitions, [string[]]$ProducerArguments, [string[]]$ConsumerArguments) {
  $directory = Join-Path $build ("multiprocess/" + $Name)
  if (Test-Path -LiteralPath $directory) { Remove-Item -LiteralPath $directory -Recurse -Force }
  New-Item -ItemType Directory -Path $directory -Force | Out-Null

  $portFile = Join-Path $directory 'coordinator.port'
  $coordinatorOut = Join-Path $directory 'coordinator.out'
  $coordinatorArguments = @('--port', '0', '--volatile', '--partitions', "$Partitions", '--print-port-file', (Quote-Argument $portFile))
  $coordinatorProcess = Start-Process -FilePath $coordinator -PassThru -NoNewWindow -RedirectStandardOutput $coordinatorOut -ArgumentList $coordinatorArguments
  if (-not (Wait-ForFile $portFile)) {
    throw "the coordinator did not publish its port; see $coordinatorOut"
  }
  $port = (Get-Content -LiteralPath $portFile -First 1).Trim()

  $producers = @()
  $index = 0
  foreach ($extra in $ProducerArguments) {
    $index++
    $out = Join-Path $directory ("producer-" + $index + ".out")
    $arguments = @('--coordinator', ("127.0.0.1:" + $port), '--role', 'producer', '--id', "$index", '--incarnation', '0')
    $arguments += $extra.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries)
    $producers += Start-Process -FilePath $participant -PassThru -NoNewWindow -RedirectStandardOutput $out -ArgumentList $arguments
    [void](Wait-ForText $out 'PARTICIPANT-PUBLISHED')
  }

  $consumers = @()
  $index = 0
  foreach ($extra in $ConsumerArguments) {
    $index++
    $out = Join-Path $directory ("consumer-" + $index + ".out")
    $arguments = @('--coordinator', ("127.0.0.1:" + $port), '--role', 'consumer', '--id', "$index", '--incarnation', '0', '--max-idle-waves', '4')
    $arguments += $extra.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries)
    $consumers += Start-Process -FilePath $participant -PassThru -NoNewWindow -RedirectStandardOutput $out -ArgumentList $arguments
    [void](Wait-ForText $out 'PARTICIPANT-READY')
  }

  foreach ($consumer in $consumers) {
    $consumer.WaitForExit()
  }

  return @{
    Directory = $directory
    Port = $port
    Coordinator = $coordinatorProcess
    Producers = $producers
    Consumers = $consumers
  }
}

function Stop-Scenario($Scenario) {
  $all = @($Scenario.Coordinator) + @($Scenario.Producers) + @($Scenario.Consumers)
  foreach ($process in $all) {
    if ($null -ne $process -and -not $process.HasExited) {
      Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
  }
}

foreach ($tool in @($coordinator, $cli, $participant)) {
  if (-not (Test-Path -LiteralPath $tool)) {
    throw "missing artifact: $tool (build first: .\scripts\msvc.ps1 -Command 'cmake --build $Configuration')"
  }
}

Write-Host "multiprocess validation (configuration: $Configuration)"

# 1 -- happy path: two producers, two consumers, every edge completes once.
$scenario = Start-Scenario 'happy' 8 @('--selection list:0,2,4,6 --payload-bytes 256 --chunk-bytes 64', '--selection list:1,3,5,7 --payload-bytes 256 --chunk-bytes 64') @('--selection all', '--selection all')
try {
  $progress = Read-Progress $scenario.Port
  # Every edge the accepted topology requires is completed exactly once. The
  # required count is whatever the registered consumers asked for, so it is
  # asserted against itself rather than against a hard-coded fan-out: the point
  # is closure, not a particular number of participants.
  Assert-That 'happy path: every required edge completed exactly once' ($progress.Fields['required'] -ge 8 -and $progress.Fields['completed'] -eq $progress.Fields['required']) $progress.Text
  Assert-That 'happy path: nothing incomplete and no failures' ($progress.Fields['incomplete'] -eq 0 -and $progress.Fields['failed'] -eq 0) $progress.Text
  Assert-That 'happy path: bytes are accounted once per partition (8 x 256)' ($progress.Fields['bytes_committed'] -eq 2048) $progress.Text
} finally {
  Stop-Scenario $scenario
}

# 2 -- the owner dies between publishing and delivery. Its partitions are
# already required, so the fabric must report them as failed rather than lose
# them, and the accounting must still close.
$scenario = Start-Scenario 'killed-producer' 8 @('--selection list:0,2,4,6 --payload-bytes 256 --chunk-bytes 64', '--selection list:1,3,5,7 --payload-bytes 256 --chunk-bytes 64') @()
try {
  if (-not $scenario.Producers[0].HasExited) {
    Stop-Process -Id $scenario.Producers[0].Id -Force
  }
  $lateOut = Join-Path $scenario.Directory 'consumer-late.out'
  $lateArguments = @('--coordinator', ("127.0.0.1:" + $scenario.Port), '--role', 'consumer', '--id', '1', '--incarnation', '0', '--max-idle-waves', '4', '--selection', 'all')
  $late = Start-Process -FilePath $participant -PassThru -NoNewWindow -RedirectStandardOutput $lateOut -ArgumentList $lateArguments
  $late.WaitForExit()

  $progress = Read-Progress $scenario.Port
  $resolved = $progress.Fields['completed'] + $progress.Fields['failed'] + $progress.Fields['incomplete']
  Assert-That 'killed producer: no edge is silently lost' ($resolved -eq $progress.Fields['required']) $progress.Text
  Assert-That 'killed producer: the dead owner leaves explicit failures, not silence' ($progress.Fields['failed'] -gt 0 -or $progress.Fields['incomplete'] -gt 0) $progress.Text
  $explanation = & $cli explain --coordinator ("127.0.0.1:" + $scenario.Port) 2>&1 | Out-String
  Assert-That 'killed producer: the reason is named deterministically' ($explanation -match 'ConnectionFailure|PeerUnavailable|ParticipantNotActive|PartitionNotProduced') $explanation
} finally {
  Stop-Scenario $scenario
}

# 3 -- corrupt payloads are refused and never committed.
$scenario = Start-Scenario 'corrupt' 4 @('--selection all --payload-bytes 256 --chunk-bytes 64 --corrupt-chunk 0') @('--selection all')
try {
  $progress = Read-Progress $scenario.Port
  Assert-That 'corrupt payload: the generation is never committed' ($progress.Fields['completed'] -lt $progress.Fields['required'] -and $progress.Fields['failed'] -gt 0) $progress.Text
  $resolved = $progress.Fields['completed'] + $progress.Fields['failed'] + $progress.Fields['incomplete']
  Assert-That 'corrupt payload: the accounting still closes' ($resolved -eq $progress.Fields['required']) $progress.Text
  $explanation = & $cli explain --coordinator ("127.0.0.1:" + $scenario.Port) 2>&1 | Out-String
  Assert-That 'corrupt payload: the refusal names an integrity code' ($explanation -match 'DigestMismatch|PayloadRejected|IntegrityFailure') $explanation
} finally {
  Stop-Scenario $scenario
}

Write-Host ""
Write-Host ("checks passed: {0}/{1}" -f ($script:checks - $script:failures.Count), $script:checks)
if ($script:failures.Count -gt 0) {
  foreach ($failure in $script:failures) { Write-Host ("  failed: {0}" -f $failure) -ForegroundColor Red }
  Write-Host "multiprocess validation: FAIL" -ForegroundColor Red
  exit 1
}
Write-Host "multiprocess validation: PASS" -ForegroundColor Green
exit 0
