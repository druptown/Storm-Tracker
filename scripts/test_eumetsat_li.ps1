[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$collectionId = "EO:EUM:DAT:0691"
$catalogUrl = "https://api.eumetsat.int/data/search-products/1.0.0/os?format=json&pi=EO%3AEUM%3ADAT%3A0691&si=0&c=1"

function ConvertTo-PlainText {
    param([Security.SecureString]$SecureValue)

    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureValue)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

try {
    Write-Host "EUMETSAT MTG Lightning Imager haalbaarheidstest" -ForegroundColor Cyan
    Write-Host "Er worden geen sleutels, tokens of databestanden opgeslagen."

    $consumerKey = Read-Host "Consumer key"
    $secureSecret = Read-Host "Consumer secret" -AsSecureString
    $consumerSecret = ConvertTo-PlainText $secureSecret
    $basicPair = [Convert]::ToBase64String(
        [Text.Encoding]::ASCII.GetBytes("${consumerKey}:${consumerSecret}")
    )

    $tokenResponse = Invoke-RestMethod `
        -Method Post `
        -Uri "https://api.eumetsat.int/token" `
        -Headers @{ Authorization = "Basic $basicPair" } `
        -Body @{ grant_type = "client_credentials"; validity_period = 3600 }

    if (-not $tokenResponse.access_token) {
        throw "Authenticatie gaf geen toegangstoken terug."
    }
    Write-Host "OK  Authenticatie" -ForegroundColor Green

    $catalog = Invoke-RestMethod -Method Get -Uri $catalogUrl
    if (-not $catalog.features -or $catalog.features.Count -eq 0) {
        throw "Collectie $collectionId bevat geen producten."
    }

    $latest = $catalog.features[0]
    $period = $latest.properties.date -split "/"
    $sensingStart = [DateTimeOffset]::Parse($period[0])
    $sensingEnd = [DateTimeOffset]::Parse($period[1])
    $published = [DateTimeOffset]::Parse($latest.properties.updated)
    $now = [DateTimeOffset]::UtcNow
    $publicationDelay = [Math]::Round(($published - $sensingEnd).TotalSeconds, 1)
    $dataAge = [Math]::Round(($now - $sensingEnd).TotalMinutes, 1)

    Write-Host "OK  Nieuwste product gevonden" -ForegroundColor Green
    Write-Host ("    Waarnemingsblok : {0:u} tot {1:u}" -f $sensingStart.UtcDateTime, $sensingEnd.UtcDateTime)
    Write-Host ("    Publicatie       : {0:u}" -f $published.UtcDateTime)
    Write-Host ("    Publicatie-delay : {0} seconden" -f $publicationDelay)
    Write-Host ("    Leeftijd data    : {0} minuten" -f $dataAge)
    Write-Host ("    Catalogusgrootte : {0} (EUMETSAT-eenheid)" -f $latest.properties.productInformation.size)

    $entries = @($latest.properties.links.'sip-entries')
    $bodyEntry = $entries | Where-Object {
        $_.title -like "*CHK-BODY*.nc"
    } | Select-Object -First 1
    if (-not $bodyEntry) {
        throw "De NetCDF BODY-entry ontbreekt in het nieuwste product."
    }

    try {
        $head = Invoke-WebRequest `
            -UseBasicParsing `
            -Method Head `
            -Uri $bodyEntry.href `
            -Headers @{ Authorization = "Bearer $($tokenResponse.access_token)" }

        $lengthHeader = $head.Headers["Content-Length"]
        if ($lengthHeader) {
            $bytes = [Int64]$lengthHeader
            Write-Host "OK  NetCDF-entry is afzonderlijk bereikbaar" -ForegroundColor Green
            Write-Host ("    Werkelijke grootte: {0:N1} MiB ({1:N0} bytes)" -f ($bytes / 1MB), $bytes)
        }
        else {
            Write-Host "LET OP  BODY-entry is bereikbaar, maar de server gaf geen Content-Length." -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "LET OP  De server ondersteunde de veilige HEAD-test niet." -ForegroundColor Yellow
        Write-Host ("    {0}" -f $_.Exception.Message)
        Write-Host "    Probeer een veilige aanvraag van uitsluitend de eerste byte..."

        Add-Type -AssemblyName System.Net.Http
        $httpClient = New-Object System.Net.Http.HttpClient
        $request = New-Object System.Net.Http.HttpRequestMessage(
            [System.Net.Http.HttpMethod]::Get,
            $bodyEntry.href
        )
        $request.Headers.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue(
            "Bearer",
            $tokenResponse.access_token
        )
        $request.Headers.Range = New-Object System.Net.Http.Headers.RangeHeaderValue(0, 0)
        try {
            $rangeResponse = $httpClient.SendAsync(
                $request,
                [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            ).GetAwaiter().GetResult()

            $totalBytes = $null
            if ($rangeResponse.Content.Headers.ContentRange) {
                $totalBytes = $rangeResponse.Content.Headers.ContentRange.Length
            }
            if (-not $totalBytes) {
                $totalBytes = $rangeResponse.Content.Headers.ContentLength
            }

            if ($totalBytes) {
                Write-Host "OK  Bestandsgrootte veilig uit responseheaders gelezen" -ForegroundColor Green
                Write-Host ("    HTTP-status      : {0}" -f [int]$rangeResponse.StatusCode)
                Write-Host ("    Werkelijke grootte: {0:N1} MiB ({1:N0} bytes)" -f ($totalBytes / 1MB), $totalBytes)
            }
            else {
                Write-Host "LET OP  Ook de byte-range-response bevat geen totale grootte." -ForegroundColor Yellow
                Write-Host ("    HTTP-status: {0}" -f [int]$rangeResponse.StatusCode)
            }
        }
        finally {
            if ($rangeResponse) { $rangeResponse.Dispose() }
            $request.Dispose()
            $httpClient.Dispose()
        }
        Write-Host "    De responsebody is niet gelezen; het grote bestand is niet gedownload."
    }

    Write-Host "Test voltooid; er is niets permanent opgeslagen." -ForegroundColor Cyan
}
finally {
    Remove-Variable consumerKey, secureSecret, consumerSecret, basicPair, tokenResponse -ErrorAction SilentlyContinue
}
