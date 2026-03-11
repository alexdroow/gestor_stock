; Script de instalación Inno Setup para Gestor de Stock Pro
; Requiere Inno Setup 6: https://jrsoftware.org/isdl.php

#define MyAppName "Gestor de Stock Pro"
#define MyAppVersion "4.85"
#define MyAppPublisher "Tu Pastelería"
#define MyAppExeName "GestionStockPro.exe"
#define MyDataDirName "GestionStockPro"
#define MyAppAssocName MyAppName + " File"
#define MyAppAssocExt ".gsp"
#define MyAppAssocKey StringChange(MyAppAssocName, " ", "") + MyAppAssocExt

[Setup]
; Información básica
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes

; Archivos de salida
OutputDir=Output
OutputBaseFilename=GestionStockPro_Setup
SetupIconFile=assets\icon.ico
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

; Permisos (admin necesario para instalar en Archivos de Programa)
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=dialog

; Información del instalador
VersionInfoVersion={#MyAppVersion}
VersionInfoCompany={#MyAppPublisher}
VersionInfoDescription=Sistema profesional de gestión de stock para pastelerías
VersionInfoCopyright=(c) 2024 {#MyAppPublisher}

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
; Archivos principales de la aplicación
Source: "dist\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion

; Nota: No incluir la base de datos existente, se creará automáticamente

[Dirs]
; Carpeta para datos del usuario (backups, base de datos)
Name: "{localappdata}\{#MyDataDirName}"; Permissions: users-full

[Icons]
; Acceso directo en el menú Inicio
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Acceso directo en el escritorio (opcional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
; Ejecutar la aplicación después de instalar
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; Limpiar archivos al desinstalar (opcional)
; Type: filesandordirs; Name: "{userappdata}\{#MyAppName}"

[Code]
// Código personalizado para el instalador

function InitializeSetup(): Boolean;
begin
  // Verificar si hay una versión anterior en ejecución
  if CheckForMutexes('GestionStockPro_Mutex') then
  begin
    MsgBox('La aplicación está en ejecución. Por favor ciérrala antes de continuar.', mbError, MB_OK);
    Result := false;
  end
  else
    Result := true;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    // Acciones posteriores a la instalación
    // Crear carpeta de datos si no existe
    ForceDirectories(ExpandConstant('{localappdata}\{#MyDataDirName}'));
  end;
end;
