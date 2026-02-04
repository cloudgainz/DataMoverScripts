[hashtable]$parameterTable = Get-AutomationVariable -Name 'SITENAME-ParameterTable' | ConvertFrom-Json -AsHashtable

function Invoke-DataMover {
    param(
        [parameter(Mandatory)]
        [hashtable]$parameterTable
    )

    # tease out parameters that I care about
    $exportStorageAccount = $parameterTable.exportStorageAccount
    $exportStorageContainer = $parameterTable.exportStorageContainer
    $exportsDirectory = $parameterTable.exportsDirectory
    $customerStorageAccount = $parameterTable.customerStorageAccount
    $customerToken = $parameterTable.customerToken
    $subscriptionName = $parameterTable.subscriptionName
    $location = $parameterTable.location
    $siteName = $parameterTable.siteName

    # Validate required parameters
    if ([string]::IsNullOrWhiteSpace($exportStorageAccount)) { throw "exportStorageAccount is required" }
    if ([string]::IsNullOrWhiteSpace($exportStorageContainer)) { throw "exportStorageContainer is required" }
    if ([string]::IsNullOrWhiteSpace($exportsDirectory)) { throw "exportsDirectory is required" }
    if ([string]::IsNullOrWhiteSpace($customerStorageAccount)) { throw "customerStorageAccount is required" }
    if ([string]::IsNullOrWhiteSpace($customerToken)) { throw "customerToken is required" }
    if ([string]::IsNullOrWhiteSpace($subscriptionName)) { throw "subscriptionName is required" }
    if ([string]::IsNullOrWhiteSpace($location)) { throw "location is required" }
    if ([string]::IsNullOrWhiteSpace($siteName)) { throw "siteName is required" }

    # Get source storage account and context
    $sourceStorageAccount = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount }
    if (-not $sourceStorageAccount) {
        throw "Export storage account '$exportStorageAccount' not found in current subscription"
    }
    $sourceContext = $sourceStorageAccount.Context

    # Create destination context using SAS token
    $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken

    # Normalize the directory path
    $normalizedPath = $exportsDirectory.Trim('/')

    # Get all blobs from source
    $allBlobs = Get-AzStorageBlob -Container $exportStorageContainer -Context $sourceContext -Prefix $normalizedPath

    # Filter out directory marker blobs (those ending with / or with 0 length that represent folders)
    $sourceBlobs = $allBlobs | Where-Object { 
        -not $_.Name.EndsWith('/') -and $_.Length -gt 0 
    }

    if (-not $sourceBlobs -or $sourceBlobs.Count -eq 0) {
        Write-Host "No blobs found in $exportStorageContainer/$normalizedPath"
        exit 0
    }

    Write-Host "Found $($sourceBlobs.Count) file blob(s) to copy (filtered out directory markers)"

    # Create destination container if needed
    $containerName = $siteName.ToLower()
    $destinationContainer = Get-AzStorageContainer -Name $containerName -Context $destinationContext -ErrorAction SilentlyContinue
    if (-not $destinationContainer) {
        Write-Host "Creating destination container: $containerName"
        New-AzStorageContainer -Name $containerName -Context $destinationContext -Permission Off | Out-Null
    }

    # Copy blobs preserving full directory structure
    $successCount = 0
    $errorCount = 0

    foreach ($blob in $sourceBlobs) {
        try {
            # Use full blob name to preserve directory structure
            $sourceBlobName = $blob.Name
            $destBlobName = $sourceBlobName  # Keeps the full path including exportsDirectory
            
            Write-Host "Copying: $sourceBlobName -> $containerName/$destBlobName"
            
            # Start async copy operation
            $copyOperation = Start-AzStorageBlobCopy `
                -Context $sourceContext `
                -SrcContainer $exportStorageContainer `
                -SrcBlob $sourceBlobName `
                -DestContext $destinationContext `
                -DestContainer $containerName `
                -DestBlob $destBlobName `
                -Force
            
            # Wait for copy to complete
            $copyState = $copyOperation | Get-AzStorageBlobCopyState -WaitForComplete
            
            if ($copyState.Status -eq "Success") {
                $successCount++
                Write-Host "  ✓ Success"
            }
            else {
                $errorCount++
                Write-Host "  ✗ Failed with status: $($copyState.Status)"
            }
        }
        catch {
            $errorCount++
            Write-Host "  ✗ Error: $($_.Exception.Message)"
        }
    }

    # Summary
    # Write-Host ""
    # Write-Host "Total blobs: $($sourceBlobs.Count)"
    # Write-Host "Successful: $successCount"
    # Write-Host "Failed: $errorCount"
}

Invoke-DataMover -parameterTable $parameterTable