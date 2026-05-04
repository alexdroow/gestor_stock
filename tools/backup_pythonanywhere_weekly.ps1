param(
  [string]$PythonAnywhereUser = "alexdroow",
  [string]$PythonAnywhereHost = "ssh.pythonanywhere.com",
  [string]$RemoteProjectDir = "/home/alexdroow/gestor_stock",
  [string]$LocalBackupRoot = "C:\\Visual Studio Code\\gestor_stock\\backups\\pythonanywhere_full",
  [int]$KeepWeeks = 26
)

$ErrorActionPreference = "Stop"

function Ensure-Command($name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "No se encontro el comando requerido: $name"
  }
}

Ensure-Command "ssh"
Ensure-Command "scp"

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$weekTag = Get-Date -Format "yyyy-'W'ww"
$backupDir = Join-Path $LocalBackupRoot "$weekTag"
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null

$remoteArchive = "/tmp/gestor_stock_full_${ts}.tar.gz"
$remoteSha = "${remoteArchive}.sha256"
$localArchive = Join-Path $backupDir "gestor_stock_full_${ts}.tar.gz"
$localSha = Join-Path $backupDir "gestor_stock_full_${ts}.tar.gz.sha256"
$manifest = Join-Path $backupDir "manifest_${ts}.json"

$remoteCmd = @"
set -e
if [ ! -d '$RemoteProjectDir' ]; then
  echo 'ERROR: no existe el directorio remoto' >&2
  exit 2
fi
cd '$RemoteProjectDir'
# Backup completo del proyecto desplegado (codigo, DB, static, templates, facturas, etc.)
tar --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' -czf '$remoteArchive' .
sha256sum '$remoteArchive' > '$remoteSha'
"@

Write-Host "[1/5] Generando archivo en PythonAnywhere..."
ssh "${PythonAnywhereUser}@${PythonAnywhereHost}" $remoteCmd

Write-Host "[2/5] Descargando backup completo a local..."
scp "${PythonAnywhereUser}@${PythonAnywhereHost}:${remoteArchive}" "$localArchive"
scp "${PythonAnywhereUser}@${PythonAnywhereHost}:${remoteSha}" "$localSha"

Write-Host "[3/5] Verificando hash..."
$shaContent = Get-Content -Raw -Path $localSha
$expected = ($shaContent -split "\s+")[0].Trim().ToLower()
$actual = (Get-FileHash -Algorithm SHA256 -Path $localArchive).Hash.ToLower()
if ($expected -ne $actual) {
  throw "Hash invalido en backup descargado. Esperado=$expected Actual=$actual"
}

Write-Host "[4/5] Guardando manifest..."
$meta = [ordered]@{
  created_at = (Get-Date).ToString("s")
  pythonanywhere_user = $PythonAnywhereUser
  pythonanywhere_host = $PythonAnywhereHost
  remote_project_dir = $RemoteProjectDir
  local_archive = $localArchive
  local_sha256 = $actual
  archive_size_bytes = (Get-Item $localArchive).Length
}
$meta | ConvertTo-Json -Depth 4 | Set-Content -Path $manifest -Encoding UTF8

Write-Host "[5/5] Limpiando temporales remotos..."
$cleanupCmd = "rm -f '$remoteArchive' '$remoteSha'"
ssh "${PythonAnywhereUser}@${PythonAnywhereHost}" $cleanupCmd | Out-Null

# Retencion
if (Test-Path $LocalBackupRoot) {
  $cutoff = (Get-Date).AddDays(-7 * [Math]::Max(1, $KeepWeeks))
  Get-ChildItem -Path $LocalBackupRoot -Directory | ForEach-Object {
    if ($_.LastWriteTime -lt $cutoff) {
      Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }
  }
}

Write-Host "Backup semanal completado: $localArchive"
