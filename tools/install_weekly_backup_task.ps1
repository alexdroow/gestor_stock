param(
  [string]$ScriptPath = "C:\\Visual Studio Code\\gestor_stock\\tools\\backup_agent_run_from_web_config.ps1",
  [string]$TaskName = "GestorStockWeeklyFullBackup",
  [string]$StartTime = "23:00"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ScriptPath)) {
  throw "No existe el script de backup: $ScriptPath"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $StartTime
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 6)

try {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
} catch {}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Backup semanal completo de PythonAnywhere para GestorStock"
Write-Host "Tarea programada creada: $TaskName (Domingos $StartTime)"
