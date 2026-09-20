<#
.SYNOPSIS
  Runs a command inside the Visual Studio x64 developer environment.

.DESCRIPTION
  Locates the installed MSVC toolchain with vswhere, imports vcvars64.bat, then
  executes the supplied command from the requested working directory. Used by the
  validation scripts and by developers so that every build uses one compiler
  environment. The exit code of the command is propagated.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true, Position = 0)][string]$Command,
  [string]$WorkDir = (Get-Location).Path
)

$ErrorActionPreference = 'Stop'

$programFilesX86 = [Environment]::GetFolderPath('ProgramFilesX86')
$vswhere = Join-Path $programFilesX86 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not (Test-Path -LiteralPath $vswhere)) {
  throw 'vswhere.exe not found; install Visual Studio Build Tools with the C++ workload.'
}

$install = (& $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath)
if (-not $install) {
  throw 'No MSVC x64 toolchain found.'
}
$install = $install.Trim()

$vcvars = Join-Path $install 'VC\Auxiliary\Build\vcvars64.bat'
if (-not (Test-Path -LiteralPath $vcvars)) {
  throw "vcvars64.bat not found at $vcvars"
}

$line = 'call "' + $vcvars + '" >nul 2>&1 && cd /d "' + $WorkDir + '" && ' + $Command
& cmd.exe /c $line
exit $LASTEXITCODE
