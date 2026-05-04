param(
  [string]$AppBaseUrl = "https://alexdroow.pythonanywhere.com",
  [string]$ProjectRoot = "C:\\Visual Studio Code\\gestor_stock"
)

$ErrorActionPreference = "Stop"

$cfgUrl = ("{0}/api/backup/orquestacion-config" -f $AppBaseUrl.TrimEnd('/'))
Write-Host "Consultando configuracion de backup en: $cfgUrl"

$resp = Invoke-RestMethod -Method Get -Uri $cfgUrl -TimeoutSec 30
if (-not $resp.success) {
  throw "No se pudo obtener configuracion remota de backup"
}

$cfg = $resp.config
if (-not $cfg.enabled) {
  Write-Host "Backup desactivado en configuracion web. No se ejecuta."
  exit 0
}

$script = Join-Path $ProjectRoot "tools\\backup_pythonanywhere_weekly.ps1"
if (-not (Test-Path $script)) {
  throw "No existe script local: $script"
}

$localRoot = [string]($cfg.local_backup_root)
if ([string]::IsNullOrWhiteSpace($localRoot)) {
  $localRoot = "C:\\Visual Studio Code\\gestor_stock\\backups\\pythonanywhere_full"
}

& powershell -NoProfile -ExecutionPolicy Bypass -File $script `
  -PythonAnywhereUser ([string]$cfg.pythonanywhere_user) `
  -PythonAnywhereHost ([string]$cfg.pythonanywhere_host) `
  -RemoteProjectDir ([string]$cfg.remote_project_dir) `
  -LocalBackupRoot $localRoot
