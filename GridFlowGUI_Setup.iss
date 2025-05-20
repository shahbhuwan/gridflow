[Setup]
AppName=GridFlow
AppVersion=1.0
AppPublisher=Bhuwan Shah
AppCopyright=Copyright (C) 2025 Bhuwan Shah
DefaultDirName={autopf}\GridFlow
DefaultGroupName=GridFlow
OutputDir=D:\GUI\gridflow\dist
OutputBaseFilename=GridFlowGUI_Setup
SetupIconFile=D:\GUI\gridflow\gridflow_logo.ico
LicenseFile=D:\GUI\gridflow\LICENSE.txt
Compression=lzma
SolidCompression=yes
WizardStyle=modern
UninstallDisplayName=GridFlow
UninstallDisplayIcon={app}\GridFlowGUI.exe

[Files]
Source: "D:\GUI\gridflow\dist\GridFlowGUI\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autodesktop}\GridFlow"; Filename: "{app}\GridFlowGUI.exe"; IconFilename: "{app}\gridflow_logo.ico"
Name: "{group}\GridFlow"; Filename: "{app}\GridFlowGUI.exe"; IconFilename: "{app}\gridflow_logo.ico"
Name: "{group}\{cm:UninstallProgram,GridFlow}"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\GridFlowGUI.exe"; Description: "{cm:LaunchProgram,GridFlow}"; Flags: nowait postinstall skipifsilent