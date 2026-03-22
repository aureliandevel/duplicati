param(
    [string]$version = "2.2.0.106",
    [string]$label   = "preview"
)
$fullVer    = "${version}_${label}"
$nugetLabel = $label -replace '_', '-'   # NuGet SemVer: alphanumeric + hyphens only
$outDir     = "publish\duplicati-$fullVer"

dotnet publish .\Executables\Duplicati.CommandLine\Duplicati.CommandLine.csproj `
    -c Release `
    -o $outDir `
    /p:AssemblyVersion=$version `
    /p:FileVersion=$version `
    /p:Version="${version}-${nugetLabel}" `
    /p:InformationalVersion=$fullVer

Compress-Archive -Path "$outDir\*" -DestinationPath "duplicati-$fullVer.zip" -Force