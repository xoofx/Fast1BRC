$inputFile = $args[0]
echo "$PSScriptRoot"
if (!$inputFile)
{
    throw "Measurements file empty. Expecting path to a file with measurements."
}
if (!(Test-Path $inputFile))
{
    throw "Measurements file not found: $inputFile"
}

echo "---------------------------------------------------------------"
echo "JIT"
echo "---------------------------------------------------------------"
dotnet build -c Release
hyperfine --warmup 2 -m 3 -M 5 ".\bin\Release\net8.0\Fast1BRC.exe $inputFile"
echo "---------------------------------------------------------------"
echo "AOT"
echo "---------------------------------------------------------------"
dotnet publish -c Release
hyperfine --warmup 2 -m 3 -M 5 ".\bin\Release\net8.0\win-x64\publish\Fast1BRC.exe $inputFile"