[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ftpRoot = "ftp://ftphsaf.meteoam.it/products/h40B/h40_cur_mon_data/"
$filePattern = '^h40_(?<date>\d{8})_(?<time>\d{4})_fdk\.nc\.gz$'

function Invoke-HsafFtpRequest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,

        [Parameter(Mandatory = $true)]
        [System.Net.NetworkCredential]$Credential,

        [Parameter(Mandatory = $true)]
        [string]$Method
    )

    $request = [System.Net.FtpWebRequest]::Create($Uri)
    $request.Method = $Method
    $request.Credentials = $Credential
    $request.EnableSsl = $true
    $request.UseBinary = $true
    $request.UsePassive = $true
    $request.KeepAlive = $false
    $request.Timeout = 30000
    $request.ReadWriteTimeout = 30000
    return $request
}

try {
    Write-Host "H SAF H40B toegangstest" -ForegroundColor Cyan
    Write-Host "Deze test gebruikt FTPS en slaat geen wachtwoord of databestand op."

    $username = Read-Host "H SAF gebruikersnaam (meestal je e-mailadres)"
    $securePassword = Read-Host "H SAF wachtwoord" -AsSecureString
    $credential = [System.Net.NetworkCredential]::new($username, $securePassword)

    $listRequest = Invoke-HsafFtpRequest `
        -Uri $ftpRoot `
        -Credential $credential `
        -Method ([System.Net.WebRequestMethods+Ftp]::ListDirectory)

    $listResponse = $listRequest.GetResponse()
    try {
        $reader = [System.IO.StreamReader]::new($listResponse.GetResponseStream())
        try {
            $entries = @($reader.ReadToEnd() -split "`r?`n" | Where-Object { $_ })
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $listResponse.Dispose()
    }

    Write-Host "OK  FTPS-authenticatie en directorytoegang werken" -ForegroundColor Green
    Write-Host ("    Directory-items : {0}" -f $entries.Count)

    $products = foreach ($entry in $entries) {
        $name = [System.IO.Path]::GetFileName($entry.TrimEnd('/'))
        if ($name -match $filePattern) {
            $timestamp = [DateTime]::ParseExact(
                "$($Matches.date)$($Matches.time)",
                "yyyyMMddHHmm",
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::AssumeUniversal -bor
                    [Globalization.DateTimeStyles]::AdjustToUniversal
            )
            [pscustomobject]@{
                Name = $name
                Timestamp = [DateTimeOffset]::new($timestamp)
            }
        }
    }

    $latest = $products | Sort-Object Timestamp -Descending | Select-Object -First 1
    if (-not $latest) {
        throw "De directory is bereikbaar, maar bevat geen H40B-bestand met de verwachte naam."
    }

    $fileUri = $ftpRoot + $latest.Name
    $sizeRequest = Invoke-HsafFtpRequest `
        -Uri $fileUri `
        -Credential $credential `
        -Method ([System.Net.WebRequestMethods+Ftp]::GetFileSize)

    $sizeResponse = $sizeRequest.GetResponse()
    try {
        $bytes = [Int64]$sizeResponse.ContentLength
    }
    finally {
        $sizeResponse.Dispose()
    }

    $ageMinutes = [Math]::Round(
        ([DateTimeOffset]::UtcNow - $latest.Timestamp).TotalMinutes,
        1
    )

    Write-Host "OK  Nieuwste H40B-product gevonden" -ForegroundColor Green
    Write-Host ("    Bestand          : {0}" -f $latest.Name)
    Write-Host ("    Producttijd UTC  : {0:u}" -f $latest.Timestamp.UtcDateTime)
    Write-Host ("    Leeftijd         : {0} minuten" -f $ageMinutes)
    Write-Host ("    Bestandsgrootte  : {0:N1} MiB ({1:N0} bytes)" -f ($bytes / 1MB), $bytes)
    Write-Host "Test voltooid; er is niets permanent opgeslagen." -ForegroundColor Cyan
}
catch {
    Write-Host "MISLUKT  H SAF H40B is nog niet bereikbaar." -ForegroundColor Red
    Write-Host ("    {0}" -f $_.Exception.Message)
    Write-Host "Controleer of het account geactiveerd is en Download Products is toegestaan."
    exit 1
}
finally {
    Remove-Variable username, securePassword, credential, entries, products, latest -ErrorAction SilentlyContinue
}
