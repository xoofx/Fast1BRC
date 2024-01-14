$inputFile = $args[0]
if (!$inputFile)
{
    throw "Measurements file empty. Expecting path to a file with measurements."
}
if (!(Test-Path $inputFile))
{
    throw "Measurements file not found: $inputFile"
}

$donet_rid = "win-x64"
if ($IsLinux)
{
    $donet_rid = "linux-x64"
}
elseif ($IsMacOS)
{
    $donet_rid = "osx-arm64"
}

echo "---------------------------------------------------------------"
echo "JIT xoofx"
echo "---------------------------------------------------------------"
dotnet build -c Release
if ($IsWindows) {
    hyperfine --warmup 2 -m 3 -M 5 ".\bin\Release\net8.0\Fast1BRC.exe $inputFile"
}
else {
    hyperfine --warmup 2 -m 3 -M 5 ".\bin\Release\net8.0\Fast1BRC $inputFile"
}
echo "---------------------------------------------------------------"
echo "AOT xoofx"
echo "---------------------------------------------------------------"
dotnet publish -c Release
if ($IsWindows) {
    hyperfine --warmup 2 -m 3 -M 5 "$PSScriptRoot\bin\Release\net8.0\$donet_rid\publish\Fast1BRC.exe $inputFile"
}
else {
    hyperfine --warmup 2 -m 3 -M 5 "$PSScriptRoot/bin/Release/net8.0/$donet_rid/publish/Fast1BRC $inputFile"
}

$testDir = "$PSScriptRoot\..\1brc-nietras"
if (Test-Path "$testDir")
{
    echo "---------------------------------------------------------------"
    echo "JIT nietras"
    echo "---------------------------------------------------------------"
    dotnet build -c Release $testDir
    if ($IsWindows) {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir\build\bin\Brc\Release\net8.0\Brc.exe $inputFile"
    }
    else {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir/build/bin/Brc/Release/net8.0/Brc $inputFile"
    }

    echo "---------------------------------------------------------------"
    echo "AOT nietras"
    echo "---------------------------------------------------------------"
    dotnet publish -p:TargetFramework=net8.0 -c Release $testDir\src\Brc\Brc.csproj
    if ($IsWindows) {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir\publish\Brc_AnyCPU_Release_net8.0_$donet_rid\Brc.exe $inputFile"
    }
    else {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir/publish/Brc_AnyCPU_Release_net8.0_$donet_rid/Brc $inputFile"
    }
}

$testDir = "$PSScriptRoot\..\1brc-buybackoff"
if (Test-Path "$testDir")
{
    echo "---------------------------------------------------------------"
    echo "JIT buybackoff"
    echo "---------------------------------------------------------------"
    dotnet build -c Release $testDir
    if ($IsWindows) {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir\1brc\bin\Release\net8.0\1brc.exe $inputFile"
    }
    else {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir/1brc/bin/Release/net8.0/1brc $inputFile"
    }
    echo "---------------------------------------------------------------"
    echo "AOT buybackoff"
    echo "---------------------------------------------------------------"
    dotnet publish -c Release
    if ($IsWindows) {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir\1brc\bin\Release\net8.0\$donet_rid\publish\1brc.exe $inputFile"
    }
    else {
        hyperfine --warmup 2 -m 3 -M 5 "$testDir/1brc/bin/Release/net8.0/$donet_rid/publish/1brc $inputFile"
    }
}