[CmdletBinding()]
param(
    [switch]$AllowPlainFtp
)

$ErrorActionPreference = "Stop"
$previousCertificateCallback = [Net.ServicePointManager]::ServerCertificateValidationCallback
$secureFtpRoot = "ftp://ftp.meteoam.it/products/h40B/h40_cur_mon_data/"
$plainFtpRoots = @(
    "ftp://ftp.meteoam.it/products/h40B/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/products/H40B/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/products/h40/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/products/H40/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/h40B/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/h40b/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/H40B/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/H40/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/h40/h40_cur_mon_data/",
    "ftp://ftp.meteoam.it/h40_cur_mon_data/",
    "ftp://ftphsaf.meteoam.it/products/h40B/h40_cur_mon_data/"
)
$filePattern = '^h40_(?<date>\d{8})_(?<time>\d{4})_fdk\.nc\.gz$'

function Invoke-HsafFtpRequest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,

        [Parameter(Mandatory = $true)]
        [System.Net.NetworkCredential]$Credential,

        [Parameter(Mandatory = $true)]
        [string]$Method,

        [bool]$EnableSsl = $true
    )

    $request = [System.Net.FtpWebRequest]::Create($Uri)
    $request.Method = $Method
    $request.Credentials = $Credential
    $request.EnableSsl = $EnableSsl
    $request.UseBinary = $true
    $request.UsePassive = $true
    $request.KeepAlive = $false
    $request.Timeout = 30000
    $request.ReadWriteTimeout = 30000
    return $request
}

try {
    Write-Host "H SAF H40B toegangstest" -ForegroundColor Cyan
    Write-Host "Deze test slaat geen wachtwoord of databestand op."

    # ftp.meteoam.it gebruikt een self-signed certificaat. Accepteer uitsluitend
    # het vooraf gecontroleerde certificaat, niet willekeurig elk ongeldig certificaat.
    [Net.ServicePointManager]::ServerCertificateValidationCallback = {
        param($sender, $certificate, $chain, $sslPolicyErrors)
        if (-not $certificate) {
            return $false
        }
        # De literal staat bewust in de callback: Windows PowerShell 5.1 voert
        # TLS-callbacks soms uit zonder toegang tot de omliggende script-scope.
        return [string]::Equals(
            $certificate.GetCertHashString(),
            "2D672F7B0D184D5BB0D510E735713D69D1C30635",
            [StringComparison]::OrdinalIgnoreCase
        )
    }

    $username = Read-Host "H SAF gebruikersnaam (meestal je e-mailadres)"
    $securePassword = Read-Host "H SAF wachtwoord" -AsSecureString
    $credential = [System.Net.NetworkCredential]::new($username, $securePassword)

    $ftpRoot = $secureFtpRoot
    try {
        $listRequest = Invoke-HsafFtpRequest `
            -Uri $ftpRoot `
            -Credential $credential `
            -Method ([System.Net.WebRequestMethods+Ftp]::ListDirectory) `
            -EnableSsl $true
        $listResponse = $listRequest.GetResponse()
        $transport = "FTPS"
    }
    catch {
        if (-not $AllowPlainFtp) {
            throw ("Veilige FTPS-test mislukte: {0} Voer alleen als je gewone FTP bewust toestaat opnieuw uit met -AllowPlainFtp." -f $_.Exception.Message)
        }

        Write-Warning "H SAF documenteert gewone FTP; gebruikersnaam en wachtwoord worden daarbij niet versleuteld verzonden."
        $lastError = $null

        # Een succesvolle root-listing maakt onderscheid tussen een geweigerde
        # login (530) en een verouderd/anders gespeld productpad (550).
        $rootEntries = @()
        $authenticatedRoot = $null
        $rootErrors = @()
        foreach ($rootCandidate in @("ftp://ftp.meteoam.it/", "ftp://ftphsaf.meteoam.it/")) {
            try {
                $rootRequest = Invoke-HsafFtpRequest `
                    -Uri $rootCandidate `
                    -Credential $credential `
                    -Method ([System.Net.WebRequestMethods+Ftp]::ListDirectory) `
                    -EnableSsl $false
                $rootResponse = $rootRequest.GetResponse()
                try {
                    $rootReader = [System.IO.StreamReader]::new($rootResponse.GetResponseStream())
                    try {
                        $rootEntries = @($rootReader.ReadToEnd() -split "`r?`n" | Where-Object { $_ })
                    }
                    finally {
                        $rootReader.Dispose()
                    }
                }
                finally {
                    $rootResponse.Dispose()
                }
                $authenticatedRoot = $rootCandidate
                break
            }
            catch {
                $rootErrors += ("{0}: {1}" -f $rootCandidate, $_.Exception.Message)
            }
        }

        if (-not $authenticatedRoot) {
            throw ("Geen FTP-hoofdmap toegankelijk. {0}" -f ($rootErrors -join " | "))
        }

        Write-Host "OK  FTP-login werkt; productmap wordt gezocht" -ForegroundColor Green
        Write-Host ("    Server          : {0}" -f $authenticatedRoot)
        Write-Host ("    Hoofdmap-items  : {0}" -f $rootEntries.Count)
        $candidateRoots = @($plainFtpRoots)
        foreach ($rootEntry in @($rootEntries | Where-Object { $_ -match '(?i)h40' })) {
            $folderName = [IO.Path]::GetFileName($rootEntry.Trim().TrimEnd('/'))
            if ($folderName) {
                $candidateRoots = @(
                    ($authenticatedRoot + $folderName + "/h40_cur_mon_data/"),
                    ($authenticatedRoot + $folderName + "/")
                ) + $candidateRoots
            }
        }
        foreach ($candidate in ($candidateRoots | Select-Object -Unique)) {
            try {
                $listRequest = Invoke-HsafFtpRequest `
                    -Uri $candidate `
                    -Credential $credential `
                    -Method ([System.Net.WebRequestMethods+Ftp]::ListDirectory) `
                    -EnableSsl $false
                $listResponse = $listRequest.GetResponse()
                $ftpRoot = $candidate
                $transport = "FTP"
                break
            }
            catch {
                $lastError = $_
            }
        }
        if (-not $listResponse) {
            $h40Entries = @($rootEntries | Where-Object { $_ -match '(?i)h40' })
            $hint = if ($h40Entries.Count) {
                " H40-items in hoofdmap: " + ($h40Entries -join ", ")
            }
            else {
                " Geen H40-map zichtbaar in de hoofdmap."
            }
            throw ("FTP-login werkt, maar geen bekend H40B-pad is toegankelijk.{0} Laatste fout: {1}" -f $hint, $lastError.Exception.Message)
        }
    }
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

    Write-Host ("OK  {0}-authenticatie en directorytoegang werken" -f $transport) -ForegroundColor Green
    Write-Host ("    Endpoint        : {0}" -f $ftpRoot)
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
        -Method ([System.Net.WebRequestMethods+Ftp]::GetFileSize) `
        -EnableSsl ($transport -eq "FTPS")

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
    [Net.ServicePointManager]::ServerCertificateValidationCallback = $previousCertificateCallback
    Remove-Variable username, securePassword, credential, entries, products, latest -ErrorAction SilentlyContinue
}
