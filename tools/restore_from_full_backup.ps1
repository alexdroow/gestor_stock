param(
  [Parameter(Mandatory=$true)]
  [string]$BackupArchive,
  [string]$RestoreTarget = "C:\\Visual Studio Code\\gestor_stock\\_restore_preview"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $BackupArchive)) {
  throw "No existe el archivo de backup: $BackupArchive"
}

New-Item -ItemType Directory -Path $RestoreTarget -Force | Out-Null

Write-Host "Extrayendo backup en: $RestoreTarget"
tar -xzf $BackupArchive -C $RestoreTarget

Write-Host "Restauracion local preparada. Revisa contenido y luego reemplaza en produccion/local segun corresponda."
Write-Host "Tip: valida stock.db y carpeta facturas antes de sobreescribir."
