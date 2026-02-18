param(
    [object] $WebhookData,
    [int] $days = 1
)

# Initialize log capture array
$script:logEntries = @()

# Custom logging function that captures to both console and array
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('Output', 'Warning', 'Error')]
        [string]$Level = 'Output'
    )
    
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $logEntry = "[$timestamp] [$Level] $Message"
    
    # Add to log array
    $script:logEntries += $logEntry
    
    # Write to console
    switch ($Level) {
        'Output'  { Write-Output "[$timestamp] $Message" }
        'Warning' { Write-Warning "[$timestamp] $Message" }
        'Error'   { Write-Error "[$timestamp] $Message" }
    }
}

# If called via webhook, extract parameters from WebhookData
if ($WebhookData) {
    Write-Log "Called via webhook"
    if ($WebhookData.RequestBody) {
        try {
            $webhookParams = $WebhookData.RequestBody | ConvertFrom-Json
            if ($webhookParams.days) {
                $days = [int]$webhookParams.days
                Write-Log "Using webhook parameter: days=$days"
            }
        }
        catch {
            Write-Log "Failed to parse webhook parameters: $($_.Exception.Message)" -Level Warning
        }
    }
}
else {
    Write-Log "Called directly (not via webhook), using days=$days"
}

# Get job ID for log filename
$jobId = $null
try {
    $jobId = $PSPrivateMetadata.JobId.Guid
    Write-Log "Job ID: $jobId"
} catch {
    Write-Log "Could not retrieve job ID: $($_.Exception.Message)" -Level Warning
}

# (get-date).ToString('o') | clip
# V 2026-02-17T18:29:21.7305131-07:00

# Authenticate using the automation account's managed identity
try {
    Write-Log "Starting runbook execution"
    Write-Log "Authenticating with managed identity..."
    Connect-AzAccount -Identity | Out-Null
    Write-Log "✓ Successfully authenticated"
}
catch {
    Write-Log "Failed to authenticate: $($_.Exception.Message)" -Level Error
    Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level Error
    throw
}

# Retrieve parameter table from automation variable
try {
    Write-Log "Retrieving parameter table from automation variable..."
    [hashtable]$parameterTable = Get-AutomationVariable -Name XXSITETABLEXX | ConvertFrom-Json -AsHashtable
    Write-Log "✓ Parameter table retrieved successfully"
}
catch {
    Write-Log "Failed to retrieve parameter table: $($_.Exception.Message)" -Level Error
    Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level Error
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
    # $transcriptOutFileName = "transcript_output_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    # $transcriptOutPath = Join-Path $env:TEMP $transcriptOutFileName
    # Start-Transcript -Path $transcriptOutPath -Force

    $jobStartTime = Get-Date
    $errors = @()
    $copiedFiles = @()
    
    try {
        Write-Log "Starting data mover operation"
        
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

        Write-Log "Configuration:"
        Write-Log "  Site: $siteName"
        Write-Log "  Source: $exportStorageAccount/$exportStorageContainer/$normalizedPath"
        Write-Log "  Destination: $customerStorageAccount/$($siteName.ToLower())"
        Write-Log "  Days: $days"

        # Validate required parameters
        Write-Log "Validating parameters..."
        if ([string]::IsNullOrWhiteSpace($exportStorageAccount)) { throw "exportStorageAccount is required" }
        if ([string]::IsNullOrWhiteSpace($exportStorageContainer)) { throw "exportStorageContainer is required" }
        if ([string]::IsNullOrWhiteSpace($exportsDirectory)) { throw "exportsDirectory is required" }
        if ([string]::IsNullOrWhiteSpace($customerStorageAccount)) { throw "customerStorageAccount is required" }
        if ([string]::IsNullOrWhiteSpace($customerToken)) { throw "customerToken is required" }
        if ([string]::IsNullOrWhiteSpace($subscriptionName)) { throw "subscriptionName is required" }
        if ([string]::IsNullOrWhiteSpace($location)) { throw "location is required" }
        if ([string]::IsNullOrWhiteSpace($siteName)) { throw "siteName is required" }
        Write-Log "✓ All parameters validated"

        # Create source storage context using managed identity
        # Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Connecting to source storage account using managed identity..."
        # $sourceContext = New-AzStorageContext -StorageAccountName $exportStorageAccount -UseConnectedAccount
        # Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Connected to source storage account"

        # Create source storage context using managed identity via Get-AzStorageAccount
        Write-Log "Retrieving storage account object for $exportStorageAccount..."
        $storageAccountObj = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount } | Select-Object -First 1
        if (-not $storageAccountObj) {
            throw "Storage account '$exportStorageAccount' not found. Ensure the Automation Account has Reader permission on the storage account."
        }
        $sourceContext = $storageAccountObj.Context
        Write-Log "✓ Using storage account context: $($storageAccountObj.Id)"

        # Create destination context using SAS token
        Write-Log "Connecting to destination storage account..."
        $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken
        Write-Log "✓ Connected to destination storage account"

        # Get all blobs from source
        Write-Log "Listing blobs in source..."
        $allBlobs = Get-AzStorageBlob -Container $exportStorageContainer -Context $sourceContext -Prefix $normalizedPath

        # Report unfiltered blob count (before pruning/filtering)
        $unfilteredCount = if ($allBlobs) { ($allBlobs | Measure-Object).Count } else { 0 }
        Write-Log "Unfiltered blobs found: $unfilteredCount in $exportStorageContainer/$normalizedPath"

        $pruneDate = (Get-Date).addDays(-$days)
        $allBlobs = $allBlobs | Where-Object {$_.LastModified -gt $pruneDate}

        $filteredCount = if ($allBlobs) { ($allBlobs | Measure-Object).Count } else { 0 }
        Write-Log "Filtered blobs found: $filteredCount in $exportStorageContainer/$normalizedPath"
        
        # Filter out directory marker blobs (those ending with / or with 0 length that represent folders)
        $sourceBlobs = $allBlobs | Where-Object { 
            -not $_.Name.EndsWith('/') -and $_.Length -gt 0 
        }

        if (-not $sourceBlobs -or $sourceBlobs.Count -eq 0) {
            Write-Log "No blobs found in $exportStorageContainer/$normalizedPath" -Level Warning
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

        Write-Log "Found $($sourceBlobs.Count) file blob(s) to copy"

        # Create destination container if needed
        $containerName = $siteName.ToLower()
        Write-Log "Checking destination container: $containerName"
        $destinationContainer = Get-AzStorageContainer -Name $containerName -Context $destinationContext -ErrorAction SilentlyContinue
        if (-not $destinationContainer) {
            Write-Log "Creating destination container: $containerName"
            New-AzStorageContainer -Name $containerName -Context $destinationContext -Permission Off | Out-Null
            Write-Log "✓ Container created"
        } else {
            Write-Log "✓ Container already exists"
        }

        # Copy blobs preserving full directory structure
        $successCount = 0
        $errorCount = 0

        Write-Log "Starting blob copy operations..."
        foreach ($blob in $sourceBlobs) {
            try {
                # Use full blob name to preserve directory structure
                $sourceBlobName = $blob.Name
                $destBlobName = $sourceBlobName  # Keeps the full path including exportsDirectory
                
                Write-Log "Copying: $sourceBlobName"
                
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
                    Write-Log "  ✓ Success"
                    $copiedFiles += $sourceBlobName
                } else {
                    $errorCount++
                    $errorMsg = "Failed with status: $($copyState.Status)"
                    Write-Log "  ✗ $errorMsg" -Level Error
                    $errors += [PSCustomObject]@{
                        File = $sourceBlobName
                        Error = $errorMsg
                    }
                }
            }
            catch {
                $errorCount++
                $errorMsg = $_.Exception.Message
                Write-Log "  ✗ Error: $errorMsg" -Level Error
                $errors += [PSCustomObject]@{
                    File = $sourceBlobName
                    Error = $errorMsg
                }
            }
        }

        # Summary
        $jobEndTime = Get-Date
        $duration = $jobEndTime - $jobStartTime
        
        Write-Log "========================================="
        Write-Log "DATA MOVER OPERATION COMPLETE"
        Write-Log "========================================="
        Write-Log "Total blobs: $($sourceBlobs.Count)"
        Write-Log "Successful: $successCount"
        Write-Log "Failed: $errorCount"
        Write-Log "Duration: $($duration.ToString())"
        Write-Log "========================================="
        
        if ($errorCount -gt 0) {
            Write-Log "Job completed with $errorCount error(s)" -Level Warning
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
        
        Write-Log "=========================================" -Level Error
        Write-Log "DATA MOVER OPERATION FAILED" -Level Error
        Write-Log "=========================================" -Level Error
        Write-Log "Error: $($_.Exception.Message)" -Level Error
        Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level Error
        Write-Log "Duration: $($duration.ToString())" -Level Error
        Write-Log "=========================================" -Level Error
        
        throw
    }

    # Stop-Transcript


    # Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Uploading Output transcript to logs container..."

    # # Create logs container if it doesn't exist
    # $logsContainer = Get-AzStorageContainer -Name 'logs' -Context $destinationContext -ErrorAction SilentlyContinue
    # if (-not $logsContainer) {
    #       try{
    #         New-AzStorageContainer -Name 'logs' -Context $destinationContext -Permission Off | Out-Null
    #       } catch {
    #         Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to create logs container: $($_.Exception.Message)"
    #       }
    # }


    # # Upload transcript
    # try {
    #     Set-AzStorageBlobContent -File $transcriptOutPath -Container 'logs' -Blob $transcriptOutFileName -Context $destinationContext -Force | Out-Null
    #     Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✓ Output transcript uploaded: $transcriptOutFileName"
    # } catch {
    #     Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to upload transcript: $($_.Exception.Message)"
    # }

    # # Clean up local transcript
    # Remove-Item -Path $transcriptOutPath -Force -ErrorAction SilentlyContinue

}

# Execute the data mover
try {
    Write-Log "Setting Azure subscription context: $($parameterTable.subscriptionName)"
    Set-AzContext -Subscription $parameterTable.subscriptionName | Out-Null
    Write-Log "✓ Subscription context set"
    
    $result = Invoke-DataMover -parameterTable $parameterTable -days $days
    
    Write-Log "Runbook execution completed successfully"
    Write-Output ($result | ConvertTo-Json -Depth 10)
}
catch {
    Write-Log "Runbook execution failed: $($_.Exception.Message)" -Level Error
    Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level Error
    throw
}
finally {
    # Upload captured log entries to storage container
    if ($parameterTable -and $script:logEntries -and $script:logEntries.Count -gt 0) {
        try {
            Write-Log "Uploading job log to storage..."
            
            # Build the complete output log
            $outputLog = @()
            $outputLog += "======================================"
            $outputLog += "Azure Automation Runbook Log"
            $outputLog += "Job ID: $jobId"
            $outputLog += "Site: $($parameterTable.siteName)"
            $outputLog += "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
            $outputLog += "======================================"
            $outputLog += ""
            $outputLog += $script:logEntries
            
            # Save to temp file
            $logFileName = "job_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
            $logPath = Join-Path $env:TEMP $logFileName
            $outputLog | Out-File -FilePath $logPath -Encoding UTF8
            
            # Upload to storage
            $customerStorageAccount = $parameterTable.customerStorageAccount
            $customerToken = $parameterTable.customerToken
            $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken

            # Create logs container if it doesn't exist
            $logsContainer = Get-AzStorageContainer -Name 'logs' -Context $destinationContext -ErrorAction SilentlyContinue
            if (-not $logsContainer) {
                New-AzStorageContainer -Name 'logs' -Context $destinationContext -Permission Off | Out-Null
            }

            # Upload job log
            Set-AzStorageBlobContent -File $logPath -Container 'logs' -Blob $logFileName -Context $destinationContext -Force | Out-Null
            Write-Log "✓ Job log uploaded: $logFileName"

            # Clean up local file
            Remove-Item -Path $logPath -Force -ErrorAction SilentlyContinue
        } catch {
            Write-Warning "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Failed to upload job log: $($_.Exception.Message)"
        }
    }
}