param(
    [object] $WebhookData,
    [int] $days = 1
)

# If called via webhook, extract parameters from WebhookData
if ($WebhookData) {
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Called via webhook"
    if ($WebhookData.RequestBody) {
        try {
            $webhookParams = $WebhookData.RequestBody | ConvertFrom-Json
            if ($webhookParams.days) {
                $days = [int]$webhookParams.days
                Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Using webhook parameter: days=$days"
            }
        }
        catch {
            Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to parse webhook parameters: $($_.Exception.Message)"
        }
    }
}
else {
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Called directly (not via webhook), using days=$days"
}

# Start transcript to capture all output
$transcriptFileName = "transcript_job_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$transcriptPath = Join-Path $env:TEMP $transcriptFileName
Start-Transcript -Path $transcriptPath -Force

# (get-date).ToString('o') | clip
# V 2026-02-12T17:56:13.2549460-07:00

# Authenticate using the automation account's managed identity
try {
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting runbook execution"
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Authenticating with managed identity..."
    Connect-AzAccount -Identity | Out-Null
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Successfully authenticated"
}
catch {
    Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to authenticate: $($_.Exception.Message)"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    throw
}

# Retrieve parameter table from automation variable
try {
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Retrieving parameter table from automation variable..."
    [hashtable]$parameterTable = Get-AutomationVariable -Name XXSITETABLEXX | ConvertFrom-Json -AsHashtable
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Parameter table retrieved successfully"
}
catch {
    Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to retrieve parameter table: $($_.Exception.Message)"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    throw
}

function Invoke-DataMover {
    param(
        [parameter(Mandatory)]
        [hashtable] $parameterTable,
        [parameter()]
        [int] $days 
    )

    # Start transcript to capture all output
    $transcriptFileName = "transcript_output_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    $transcriptPath = Join-Path $env:TEMP $transcriptFileName
    Start-Transcript -Path $transcriptPath -Force

    $jobStartTime = Get-Date
    $errors = @()
    $copiedFiles = @()
    
    try {
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting data mover operation"
        
        # tease out parameters that I care about
        $exportStorageAccount = $parameterTable.exportStorageAccount
        $exportStorageContainer = $parameterTable.exportStorageContainer
        $exportsDirectory = $parameterTable.exportsDirectory
        $customerStorageAccount = $parameterTable.customerStorageAccount
        $customerToken = $parameterTable.customerToken
        $subscriptionName = $parameterTable.subscriptionName
        $location = $parameterTable.location
        $siteName = $parameterTable.siteName

        $normalizedPath = $exportsDirectory.Trim('/')

        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Configuration:"
        Write-Output "  Site: $siteName"
        Write-Output "  Source: $exportStorageAccount/$exportStorageContainer/$normalizedPath"
        Write-Output "  Destination: $customerStorageAccount/$($siteName.ToLower())"
        Write-Output "  Days: $days"

        # Validate required parameters
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Validating parameters..."
        if ([string]::IsNullOrWhiteSpace($exportStorageAccount)) { throw "exportStorageAccount is required" }
        if ([string]::IsNullOrWhiteSpace($exportStorageContainer)) { throw "exportStorageContainer is required" }
        if ([string]::IsNullOrWhiteSpace($exportsDirectory)) { throw "exportsDirectory is required" }
        if ([string]::IsNullOrWhiteSpace($customerStorageAccount)) { throw "customerStorageAccount is required" }
        if ([string]::IsNullOrWhiteSpace($customerToken)) { throw "customerToken is required" }
        if ([string]::IsNullOrWhiteSpace($subscriptionName)) { throw "subscriptionName is required" }
        if ([string]::IsNullOrWhiteSpace($location)) { throw "location is required" }
        if ([string]::IsNullOrWhiteSpace($siteName)) { throw "siteName is required" }
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ All parameters validated"

        # Create source storage context using managed identity
        # Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Connecting to source storage account using managed identity..."
        # $sourceContext = New-AzStorageContext -StorageAccountName $exportStorageAccount -UseConnectedAccount
        # Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Connected to source storage account"

        # Create source storage context using managed identity via Get-AzStorageAccount
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Retrieving storage account object for $exportStorageAccount..."
        $storageAccountObj = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount } | Select-Object -First 1
        if (-not $storageAccountObj) {
            throw "Storage account '$exportStorageAccount' not found. Ensure the Automation Account has Reader permission on the storage account."
        }
        $sourceContext = $storageAccountObj.Context
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Using storage account context: $($storageAccountObj.Id)"

        # Create destination context using SAS token
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Connecting to destination storage account..."
        $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Connected to destination storage account"

        # Get all blobs from source
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Listing blobs in source..."
        $allBlobs = Get-AzStorageBlob -Container $exportStorageContainer -Context $sourceContext -Prefix $normalizedPath

        # Report unfiltered blob count (before pruning/filtering)
        $unfilteredCount = if ($allBlobs) { ($allBlobs | Measure-Object).Count } else { 0 }
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Unfiltered blobs found: $unfilteredCount in $exportStorageContainer/$normalizedPath"

        $pruneDate = (Get-Date).addDays(-$days)
        $allBlobs = $allBlobs | Where-Object {$_.LastModified -gt $pruneDate}

        $filteredCount = if ($allBlobs) { ($allBlobs | Measure-Object).Count } else { 0 }
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Filtered blobs found: $filteredCount in $exportStorageContainer/$normalizedPath"
        
        # Filter out directory marker blobs (those ending with / or with 0 length that represent folders)
        $sourceBlobs = $allBlobs | Where-Object { 
            -not $_.Name.EndsWith('/') -and $_.Length -gt 0 
        }

        if (-not $sourceBlobs -or $sourceBlobs.Count -eq 0) {
            Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] No blobs found in $exportStorageContainer/$normalizedPath"
            return [PSCustomObject]@{
                Status = "Success"
                Message = "No files to copy"
                TotalBlobs = 0
                Successful = 0
                Failed = 0
                Duration = ((Get-Date) - $jobStartTime).ToString()
                Errors = @()
                CopiedFiles = @()
            }
        }

        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Found $($sourceBlobs.Count) file blob(s) to copy"

        # Create destination container if needed
        $containerName = $siteName.ToLower()
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Checking destination container: $containerName"
        $destinationContainer = Get-AzStorageContainer -Name $containerName -Context $destinationContext -ErrorAction SilentlyContinue
        if (-not $destinationContainer) {
            Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Creating destination container: $containerName"
            New-AzStorageContainer -Name $containerName -Context $destinationContext -Permission Off | Out-Null
            Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Container created"
        } else {
            Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Container already exists"
        }

        # Copy blobs preserving full directory structure
        $successCount = 0
        $errorCount = 0

        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting blob copy operations..."
        foreach ($blob in $sourceBlobs) {
            try {
                # Use full blob name to preserve directory structure
                $sourceBlobName = $blob.Name
                $destBlobName = $sourceBlobName  # Keeps the full path including exportsDirectory
                
                Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Copying: $sourceBlobName"
                
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
                    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]   ✓ Success"
                    $copiedFiles += $sourceBlobName
                } else {
                    $errorCount++
                    $errorMsg = "Failed with status: $($copyState.Status)"
                    Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]   ✗ $errorMsg"
                    $errors += [PSCustomObject]@{
                        File = $sourceBlobName
                        Error = $errorMsg
                    }
                }
            }
            catch {
                $errorCount++
                $errorMsg = $_.Exception.Message
                Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]   ✗ Error: $errorMsg"
                $errors += [PSCustomObject]@{
                    File = $sourceBlobName
                    Error = $errorMsg
                }
            }
        }

        # Summary
        $jobEndTime = Get-Date
        $duration = $jobEndTime - $jobStartTime
        
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] DATA MOVER OPERATION COMPLETE"
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Total blobs: $($sourceBlobs.Count)"
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Successful: $successCount"
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed: $errorCount"
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Duration: $($duration.ToString())"
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        
        if ($errorCount -gt 0) {
            Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Job completed with $errorCount error(s)"
        }

        return [PSCustomObject]@{
            Status = if ($errorCount -eq 0) { "Success" } else { "PartialSuccess" }
            Message = "Copied $successCount of $($sourceBlobs.Count) files"
            TotalBlobs = $sourceBlobs.Count
            Successful = $successCount
            Failed = $errorCount
            Duration = $duration.ToString()
            Errors = $errors
            CopiedFiles = $copiedFiles
        }
    }
    catch {
        $jobEndTime = Get-Date
        $duration = $jobEndTime - $jobStartTime
        
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] DATA MOVER OPERATION FAILED"
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Error: $($_.Exception.Message)"
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Stack trace: $($_.ScriptStackTrace)"
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Duration: $($duration.ToString())"
        Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ========================================="
        
        throw
    }

    Stop-Transcript

    try {
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Uploading transcript to logs container..."

        # Create logs container if it doesn't exist
        $logsContainer = Get-AzStorageContainer -Name 'logs' -Context $destinationContext -ErrorAction SilentlyContinue
        if (-not $logsContainer) {
            New-AzStorageContainer -Name 'logs' -Context $destinationContext -Permission Off | Out-Null
        }

        # Upload transcript
        Set-AzStorageBlobContent -File $transcriptPath -Container 'logs' -Blob $transcriptFileName -Context $destinationContext -Force | Out-Null
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Transcript uploaded: $transcriptFileName"

        # Clean up local transcript
        Remove-Item -Path $transcriptPath -Force -ErrorAction SilentlyContinue
    } catch {
        Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to upload transcript: $($_.Exception.Message)"
    }

}

# Execute the data mover
try {
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Setting Azure subscription context: $($parameterTable.subscriptionName)"
    Set-AzContext -Subscription $parameterTable.subscriptionName | Out-Null
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Subscription context set"
    
    $result = Invoke-DataMover -parameterTable $parameterTable -days $days
    
    Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Runbook execution completed successfully"
    Write-Output ($result | ConvertTo-Json -Depth 10)
}
catch {
    Write-Error "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Runbook execution failed: $($_.Exception.Message)"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    throw
}
finally {
    # Stop transcript and upload to logs container
    try {
        Stop-Transcript
    } catch {
        Write-Warning "Failed to stop transcript: $($_.Exception.Message)"
    }

    try {
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Uploading transcript to logs container..."
        $customerStorageAccount = $parameterTable.customerStorageAccount
        $customerToken = $parameterTable.customerToken
        $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken

        # Create logs container if it doesn't exist
        $logsContainer = Get-AzStorageContainer -Name 'logs' -Context $destinationContext -ErrorAction SilentlyContinue
        if (-not $logsContainer) {
            New-AzStorageContainer -Name 'logs' -Context $destinationContext -Permission Off | Out-Null
        }

        # Upload transcript
        Set-AzStorageBlobContent -File $transcriptPath -Container 'logs' -Blob $transcriptFileName -Context $destinationContext -Force | Out-Null
        Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Transcript uploaded: $transcriptFileName"

        # Clean up local transcript
        Remove-Item -Path $transcriptPath -Force -ErrorAction SilentlyContinue
    } catch {
        Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to upload transcript: $($_.Exception.Message)"
    }
}