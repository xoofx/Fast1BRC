dotnet build -c Release
$env:DOTNET_EnableEventPipe=1
$env:DOTNET_EventPipeConfig="Microsoft-Windows-DotNETRuntime:0x1F000080018:5"
$env:DOTNET_EventPipeOutputPath="trace.nettrace"
$env:DOTNET_TieredPGO=1
$env:DOTNET_ReadyToRun=0
$env:DOTNET_TC_QuickJitForLoops=1
.\bin\Release\net8.0\Fast1BRC.exe .\measurements3.txt --pgo --time
$env:DOTNET_EnableEventPipe=0
dotnet-pgo create-mibc -t trace.nettrace -o pgo.mibc
del .\trace.nettrace
del .\trace.etlx