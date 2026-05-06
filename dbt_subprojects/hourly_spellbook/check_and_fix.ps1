
$stashedFiles = Get-ChildItem -Path "../../logs/temp_stash_part3/*.sql" | Select-Object -ExpandProperty Name
$baseDir = "models/_project/gmx/event"
$countWithMegaeth = 0

foreach ($file in $stashedFiles) {
    # e.g., "gmx_v2_megaeth_claim_funds_claimed.sql" -> "gmx_v2_claim_funds_claimed.sql"
    $genericName = $file.Replace("_megaeth", "")
    $genericPath = Join-Path $baseDir $genericName
    
    if (Test-Path $genericPath) {
        $content = Get-Content $genericPath -Raw
        if ($content -match "megaeth") {
            $countWithMegaeth++
            Write-Host "Found megaeth in $genericName"
            
            # Fix it
            $newContent = $content -replace ",\s*'megaeth'", ""
            $newContent = $newContent -replace "'megaeth'\s*,", ""
            $newContent = $newContent -replace "\[\s*'megaeth'\s*\]", "[]"
            $newContent = $newContent -replace "'megaeth'\s*", ""
            
            if ($content -cne $newContent) {
                Set-Content -Path $genericPath -Value $newContent -NoNewline
                Write-Host "- Removed megaeth from $genericName"
            }
        }
    }
}
Write-Host "Processed $countWithMegaeth files."

