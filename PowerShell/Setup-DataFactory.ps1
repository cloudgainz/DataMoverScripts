param(
    [parameter(Mandatory)]
    [hashtable]$parameterTable
)

# ============================================================================
# Setup-DataFactory.ps1
# Creates an Azure Data Factory with a scheduled pipeline to copy blobs from
# a source storage account to a destination storage account.
#
# Source storage is accessed via the ADF's system-assigned managed identity (RBAC).
# Destination storage is accessed via SAS token.
#
# If vnetSubnetId is provided, the source storage account is assumed to be
# behind a private endpoint. ADF is configured with a Managed Virtual Network
# and a Managed Private Endpoint to reach it.
#
# If vnetSubnetId is NOT provided, ADF connects to the source storage directly.
# ============================================================================

# tease out parameters that I care about
$location = $parameterTable.location
$siteName = $parameterTable.siteName
$ResourceGroupName = $parameterTable.runBookRG
$subscriptionName = $parameterTable.subscriptionName
$vnetSubnetId = $parameterTable.vnetSubnetId
$exportStorageAccount = $parameterTable.exportStorageAccount
$exportStorageContainer = $parameterTable.exportStorageContainer
$exportsDirectory = $parameterTable.exportsDirectory
$customerStorageAccount = $parameterTable.customerStorageAccount
$customerToken = $parameterTable.customerToken
$days = if ($parameterTable.days) { [int]$parameterTable.days } else { 7 }

Set-AzContext -Subscription $subscriptionName

# Naming conventions
[string]$DataFactoryName = $siteName + "-adf"
[string]$PipelineName = $siteName + "-DataMoverPipeline"
[string]$TriggerName = $siteName + "-DailyTrigger"
[string]$SourceLinkedServiceName = "SourceStorage"
[string]$DestLinkedServiceName = "DestStorage"
[string]$SourceDatasetName = "SourceBlobs"
[string]$DestDatasetName = "DestBlobs"
[string]$IRName = $siteName + "-ManagedIR"
[string]$ManagedPEName = $siteName + "-SourceStoragePE"
[hashtable]$Tags = @{}

$normalizedPath = $exportsDirectory.Trim('/')
$destContainerName = $siteName.ToLower()
$usePrivateEndpoint = -not [string]::IsNullOrWhiteSpace($vnetSubnetId)

[DateTime]$ScheduleStartTime = (Get-Date).AddDays(1).Date.AddHours(2)

# Webhook / remote-trigger variables (populated in Step 9)
$webhookSPName      = $null
$webhookTenantId    = $null
$webhookClientId    = $null
$webhookClientSecret = $null
$webhookTriggerUri  = $null

if ($usePrivateEndpoint) {
    Write-Host "Mode: Managed VNet + Private Endpoint (vnetSubnetId detected)" -ForegroundColor Magenta
    Write-Host "  Subnet: $vnetSubnetId" -ForegroundColor Gray
} else {
    Write-Host "Mode: Standard Data Factory (no private endpoint)" -ForegroundColor Magenta
}

# Set error action preference
$ErrorActionPreference = "Stop"

# Import required modules
Write-Host "Checking for required Azure modules..." -ForegroundColor Cyan
$requiredModules = @("Az.Accounts", "Az.DataFactory", "Az.Resources", "Az.Storage")
if ($usePrivateEndpoint) {
    $requiredModules += @("Az.Network")
}

foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Host "Installing module: $module" -ForegroundColor Yellow
        Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
    }
    Import-Module $module -Force
}

# Connect to Azure (if not already connected)
Write-Host "Checking Azure connection..." -ForegroundColor Cyan
$context = Get-AzContext
if (-not $context) {
    Write-Host "Connecting to Azure..." -ForegroundColor Yellow
    Connect-AzAccount
}

Write-Host "Connected to Azure subscription: $($context.Subscription.Name)" -ForegroundColor Green
$subscriptionId = $context.Subscription.Id

# Helper: create a temp JSON file from a hashtable (for ADF cmdlet -DefinitionFile params)
function New-TempAdfJson {
    param([hashtable]$Definition)
    $tempFile = Join-Path $env:TEMP "adf_$(New-Guid).json"
    $Definition | ConvertTo-Json -Depth 20 | Out-File -FilePath $tempFile -Encoding utf8 -Force
    return $tempFile
}

try {
    # =========================================================================
    # Step 1: Create or verify Resource Group
    # =========================================================================
    Write-Host "`n[1] Checking Resource Group: $ResourceGroupName" -ForegroundColor Cyan
    $rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
    if (-not $rg) {
        Write-Host "Creating Resource Group: $ResourceGroupName in $location" -ForegroundColor Yellow
        New-AzResourceGroup -Name $ResourceGroupName -Location $location -Tag $Tags | Out-Null
        Write-Host "✓ Resource Group created successfully" -ForegroundColor Green
    } else {
        Write-Host "✓ Resource Group already exists" -ForegroundColor Green
    }

    # =========================================================================
    # Step 2: Create Data Factory with system-assigned managed identity
    # =========================================================================
    Write-Host "`n[2] Creating Data Factory: $DataFactoryName" -ForegroundColor Cyan
    $existingAdf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue

    if (-not $existingAdf) {
        # Use REST API to guarantee system-assigned managed identity is enabled
        Write-Host "Provisioning Data Factory..." -ForegroundColor Yellow
        $factoryPath = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/${DataFactoryName}?api-version=2018-06-01"
        $factoryBody = @{
            location   = $location
            tags       = $Tags
            identity   = @{ type = "SystemAssigned" }
            properties = @{}
        } | ConvertTo-Json -Depth 10

        $factoryResult = Invoke-AzRestMethod -Path $factoryPath -Method PUT -Payload $factoryBody
        if ($factoryResult.StatusCode -lt 200 -or $factoryResult.StatusCode -ge 300) {
            throw "Failed to create Data Factory: $($factoryResult.Content)"
        }

        Start-Sleep -Seconds 10

        $adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue
        while (-not $adf) {
            Write-Host "Waiting for Data Factory to be provisioned..." -ForegroundColor Yellow
            Start-Sleep -Seconds 5
            $adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue
        }

        Write-Host "✓ Data Factory created successfully" -ForegroundColor Green
    } else {
        $adf = $existingAdf
        Write-Host "✓ Data Factory already exists" -ForegroundColor Green
    }

    $principalId = $adf.Identity.PrincipalId
    if (-not $principalId) {
        throw "Data Factory managed identity is not enabled. Principal ID is null."
    }
    Write-Host "  Principal ID: $principalId" -ForegroundColor Gray

    # =========================================================================
    # Step 3: Configure Managed Identity RBAC on source storage account
    # =========================================================================
    Write-Host "`n[3] Configuring RBAC on source storage: $exportStorageAccount" -ForegroundColor Cyan

    $storageAccount = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount } | Select-Object -First 1
    if (-not $storageAccount) {
        throw "Source storage account '$exportStorageAccount' not found in subscription '$subscriptionName'"
    }

    Write-Host "  Storage Account ID: $($storageAccount.Id)" -ForegroundColor Gray

    $rolesToAssign = @("Reader", "Storage Blob Data Reader", "Storage Account Contributor")
    foreach ($roleName in $rolesToAssign) {
        $existingAssignment = Get-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName $roleName -Scope $storageAccount.Id -ErrorAction SilentlyContinue
        if (-not $existingAssignment) {
            $tryTotal = 5
            $tryCount = 0
            while ($tryCount -lt $tryTotal) {
                try {
                    New-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName $roleName -Scope $storageAccount.Id | Out-Null
                    Write-Host "✓ $roleName role assigned" -ForegroundColor Green
                    break
                } catch {
                    $tryCount++
                    if ($tryCount -ge $tryTotal) {
                        throw "Failed to assign $roleName after $tryTotal attempts: $_"
                    }
                    Write-Host "  Retrying $roleName assignment... (Attempt $tryCount of $tryTotal)" -ForegroundColor Yellow
                    Start-Sleep -Seconds 5
                }
            }
        } else {
            Write-Host "✓ $roleName role already assigned" -ForegroundColor Green
        }
    }

    # =========================================================================
    # Step 4: (Private Endpoint only) Managed VNet, Integration Runtime, and
    #         Managed Private Endpoint
    # =========================================================================
    if ($usePrivateEndpoint) {
        Write-Host "`n[4] Setting up Managed VNet and Private Endpoint" -ForegroundColor Cyan

        # 4a: Create Managed Virtual Network
        Write-Host "Creating Managed Virtual Network..." -ForegroundColor Yellow
        $vnetPath = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/managedVirtualNetworks/default?api-version=2018-06-01"
        $vnetBody = @{ properties = @{} } | ConvertTo-Json
        $vnetResult = Invoke-AzRestMethod -Path $vnetPath -Method PUT -Payload $vnetBody
        if ($vnetResult.StatusCode -lt 200 -or $vnetResult.StatusCode -ge 300) {
            throw "Failed to create Managed Virtual Network: $($vnetResult.Content)"
        }
        Write-Host "✓ Managed Virtual Network created" -ForegroundColor Green

        # 4b: Create Integration Runtime in the Managed VNet
        Write-Host "Creating Integration Runtime: $IRName..." -ForegroundColor Yellow
        $irPath = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/integrationRuntimes/${IRName}?api-version=2018-06-01"
        $irBody = @{
            properties = @{
                type           = "Managed"
                typeProperties = @{
                    computeProperties = @{
                        location = "AutoResolve"
                    }
                }
                managedVirtualNetwork = @{
                    type          = "ManagedVirtualNetworkReference"
                    referenceName = "default"
                }
            }
        } | ConvertTo-Json -Depth 10

        $irResult = Invoke-AzRestMethod -Path $irPath -Method PUT -Payload $irBody
        if ($irResult.StatusCode -lt 200 -or $irResult.StatusCode -ge 300) {
            throw "Failed to create Integration Runtime: $($irResult.Content)"
        }
        Write-Host "✓ Integration Runtime created" -ForegroundColor Green

        # 4c: Create Managed Private Endpoint to source storage account
        Write-Host "Creating Managed Private Endpoint: $ManagedPEName..." -ForegroundColor Yellow
        $pePath = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/managedVirtualNetworks/default/managedPrivateEndpoints/${ManagedPEName}?api-version=2018-06-01"
        $peBody = @{
            properties = @{
                privateLinkResourceId = $storageAccount.Id
                groupId               = "blob"
            }
        } | ConvertTo-Json -Depth 5

        $peResult = Invoke-AzRestMethod -Path $pePath -Method PUT -Payload $peBody
        if ($peResult.StatusCode -lt 200 -or $peResult.StatusCode -ge 300) {
            throw "Failed to create Managed Private Endpoint: $($peResult.Content)"
        }
        Write-Host "✓ Managed Private Endpoint created" -ForegroundColor Green

        # 4d: Auto-approve the private endpoint connection on the storage account
        Write-Host "Waiting for private endpoint connection to appear on storage account..." -ForegroundColor Yellow
        Start-Sleep -Seconds 15

        $approved = $false
        for ($attempt = 1; $attempt -le 12; $attempt++) {
            $connections = Get-AzPrivateEndpointConnection -PrivateLinkResourceId $storageAccount.Id -ErrorAction SilentlyContinue
            $pendingConnection = $connections | Where-Object {
                $_.PrivateLinkServiceConnectionState.Status -eq "Pending"
            } | Select-Object -First 1

            if ($pendingConnection) {
                Write-Host "Approving private endpoint connection..." -ForegroundColor Yellow
                Approve-AzPrivateEndpointConnection -ResourceId $pendingConnection.Id | Out-Null
                $approved = $true
                Write-Host "✓ Private endpoint connection approved" -ForegroundColor Green
                break
            }
            Write-Host "  Connection not ready, retrying in 10s... ($attempt/12)" -ForegroundColor Yellow
            Start-Sleep -Seconds 10
        }

        if (-not $approved) {
            Write-Host "⚠ Could not auto-approve the private endpoint connection." -ForegroundColor Yellow
            Write-Host "  Please manually approve it in the Azure portal:" -ForegroundColor Yellow
            Write-Host "  Storage Account '$exportStorageAccount' → Networking → Private endpoint connections" -ForegroundColor Yellow
        }
    } else {
        Write-Host "`n[4] Skipping Managed VNet setup (no private endpoint needed)" -ForegroundColor Cyan
    }

    # =========================================================================
    # Step 5: Create Linked Services
    # =========================================================================
    Write-Host "`n[5] Creating Linked Services" -ForegroundColor Cyan

    # Source linked service — managed identity auth via serviceEndpoint
    Write-Host "Creating source linked service: $SourceLinkedServiceName..." -ForegroundColor Yellow
    $sourceLinkedServiceDef = @{
        name       = $SourceLinkedServiceName
        properties = @{
            type           = "AzureBlobStorage"
            typeProperties = @{
                serviceEndpoint = "https://$exportStorageAccount.blob.core.windows.net/"
            }
        }
    }
    if ($usePrivateEndpoint) {
        $sourceLinkedServiceDef.properties["connectVia"] = @{
            referenceName = $IRName
            type          = "IntegrationRuntimeReference"
        }
    }
    $tmpFile = New-TempAdfJson -Definition $sourceLinkedServiceDef
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $SourceLinkedServiceName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Source linked service created (managed identity)" -ForegroundColor Green

    # Destination linked service — SAS token auth
    Write-Host "Creating destination linked service: $DestLinkedServiceName..." -ForegroundColor Yellow
    $sasUri = "https://$customerStorageAccount.blob.core.windows.net/?$customerToken"
    $destLinkedServiceDef = @{
        name       = $DestLinkedServiceName
        properties = @{
            type           = "AzureBlobStorage"
            typeProperties = @{
                sasUri = @{
                    type  = "SecureString"
                    value = $sasUri
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $destLinkedServiceDef
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $DestLinkedServiceName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Destination linked service created (SAS token)" -ForegroundColor Green

    # =========================================================================
    # Step 6: Create Datasets
    # =========================================================================
    Write-Host "`n[6] Creating Datasets" -ForegroundColor Cyan

    # Source dataset — Binary format, preserves file structure as-is
    Write-Host "Creating source dataset: $SourceDatasetName..." -ForegroundColor Yellow
    $sourceDatasetDef = @{
        name       = $SourceDatasetName
        properties = @{
            type              = "Binary"
            linkedServiceName = @{
                referenceName = $SourceLinkedServiceName
                type          = "LinkedServiceReference"
            }
            typeProperties = @{
                location = @{
                    type       = "AzureBlobStorageLocation"
                    container  = $exportStorageContainer
                    folderPath = $normalizedPath
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $sourceDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $SourceDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Source dataset created ($exportStorageContainer/$normalizedPath)" -ForegroundColor Green

    # Destination dataset — Binary format, mirrors source folder structure
    Write-Host "Creating destination dataset: $DestDatasetName..." -ForegroundColor Yellow
    $destDatasetDef = @{
        name       = $DestDatasetName
        properties = @{
            type              = "Binary"
            linkedServiceName = @{
                referenceName = $DestLinkedServiceName
                type          = "LinkedServiceReference"
            }
            typeProperties = @{
                location = @{
                    type       = "AzureBlobStorageLocation"
                    container  = $destContainerName
                    folderPath = $normalizedPath
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $destDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $DestDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Destination dataset created ($destContainerName/$normalizedPath)" -ForegroundColor Green

    # =========================================================================
    # Step 7: Create Pipeline
    # =========================================================================
    Write-Host "`n[7] Creating Pipeline: $PipelineName" -ForegroundColor Cyan

    # The pipeline has a 'days' parameter (default 7).
    # The Copy activity filters source blobs by LastModified >= (now - days).
    # Binary copy with recursive=true preserves full folder hierarchy.
    # After the copy, two Web activities write a run-summary JSON blob to
    # customerStorageAccount/logs/{siteName}-{RunId}.json via the SAS token.
    #
    # Body objects use ADF @{expression} interpolation inside string values —
    # ADF substitutes each @{...} at runtime, so no concat() expressions needed.
    # PowerShell serializes these hashtables to JSON via New-TempAdfJson.
    $logBlobUrlExpr = "@concat('https://$customerStorageAccount.blob.core.windows.net/logs/$siteName-', pipeline().RunId, '.json?$customerToken')"

    $successBody = @{
        runId               = "@{pipeline().RunId}"
        pipeline            = "@{pipeline().Pipeline}"
        triggerTime         = "@{string(pipeline().TriggerTime)}"
        status              = "Succeeded"
        dataReadBytes       = "@{activity('CopyBlobs').output.dataRead}"
        dataWrittenBytes    = "@{activity('CopyBlobs').output.dataWritten}"
        filesRead           = "@{activity('CopyBlobs').output.filesRead}"
        filesWritten        = "@{activity('CopyBlobs').output.filesWritten}"
        copyDurationSeconds = "@{activity('CopyBlobs').output.copyDuration}"
        throughputMBps      = "@{activity('CopyBlobs').output.throughput}"
    }

    $failureBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "Failed"
        errorCode    = "@{activity('CopyBlobs').error.errorCode}"
        errorMessage = "@{activity('CopyBlobs').error.message}"
    }

    # Body for when the logging activity itself errors (CopyBlobs succeeded but WriteSuccessLog failed)
    $logErrorBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "LoggingError"
        errorCode    = "@{activity('WriteSuccessLog').error.errorCode}"
        errorMessage = "@{activity('WriteSuccessLog').error.message}"
        note         = "CopyBlobs succeeded but the success log write failed"
    }

    $pipelineDef = @{
        name       = $PipelineName
        properties = @{
            parameters = @{
                days = @{
                    type         = "Int"
                    defaultValue = $days
                }
            }
            activities = @(
                @{
                    name    = "CopyBlobs"
                    type    = "Copy"
                    inputs  = @(
                        @{
                            referenceName = $SourceDatasetName
                            type          = "DatasetReference"
                        }
                    )
                    outputs = @(
                        @{
                            referenceName = $DestDatasetName
                            type          = "DatasetReference"
                        }
                    )
                    typeProperties = @{
                        source = @{
                            type          = "BinarySource"
                            storeSettings = @{
                                type                      = "AzureBlobStorageReadSettings"
                                recursive                 = $true
                                modifiedDatetimeStart     = @{
                                    value = "@adddays(utcnow(), sub(0, pipeline().parameters.days))"
                                    type  = "Expression"
                                }
                                deleteFilesAfterCompletion = $false
                            }
                        }
                        sink = @{
                            type          = "BinarySink"
                            storeSettings = @{
                                type = "AzureBlobStorageWriteSettings"
                            }
                        }
                    }
                },
                # ------------------------------------------------------------------
                # WriteSuccessLog: PUT a JSON summary blob on successful copy
                # ------------------------------------------------------------------
                @{
                    name      = "WriteSuccessLog"
                    type      = "WebActivity"
                    dependsOn = @(
                        @{ activity = "CopyBlobs"; dependencyConditions = @("Succeeded") }
                    )
                    typeProperties = @{
                        url    = @{ value = $logBlobUrlExpr; type = "Expression" }
                        method = "PUT"
                        headers = @{
                            "x-ms-blob-type" = "BlockBlob"
                            "Content-Type"   = "application/json"
                        }
                        body = $successBody
                    }
                },
                # ------------------------------------------------------------------
                # WriteFailureLog: PUT a JSON error summary blob on failed copy
                # ------------------------------------------------------------------
                @{
                    name      = "WriteFailureLog"
                    type      = "WebActivity"
                    dependsOn = @(
                        @{ activity = "CopyBlobs"; dependencyConditions = @("Failed") }
                    )
                    typeProperties = @{
                        url    = @{ value = $logBlobUrlExpr; type = "Expression" }
                        method = "PUT"
                        headers = @{
                            "x-ms-blob-type" = "BlockBlob"
                            "Content-Type"   = "application/json"
                        }
                        body = $failureBody
                    }
                },
                # ------------------------------------------------------------------
                # WriteLogError: fallback if WriteSuccessLog itself fails
                # ------------------------------------------------------------------
                @{
                    name      = "WriteLogError"
                    type      = "WebActivity"
                    dependsOn = @(
                        @{ activity = "WriteSuccessLog"; dependencyConditions = @("Failed") }
                    )
                    typeProperties = @{
                        url    = @{ value = $logBlobUrlExpr; type = "Expression" }
                        method = "PUT"
                        headers = @{
                            "x-ms-blob-type" = "BlockBlob"
                            "Content-Type"   = "application/json"
                        }
                        body = $logErrorBody
                    }
                }
            )
        }
    }
    $tmpFile = New-TempAdfJson -Definition $pipelineDef
    Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $PipelineName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Pipeline created (copies blobs modified in last $days days)" -ForegroundColor Green

    # =========================================================================
    # Step 8: Create and Start Daily Trigger
    # =========================================================================
    Write-Host "`n[8] Creating Trigger: $TriggerName" -ForegroundColor Cyan

    # Stop existing trigger if it's running (required before update)
    $existingTrigger = Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -ErrorAction SilentlyContinue
    if ($existingTrigger -and $existingTrigger.RuntimeState -eq "Started") {
        Write-Host "Stopping existing trigger before update..." -ForegroundColor Yellow
        Stop-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -Force | Out-Null
    }

    $triggerDef = @{
        name       = $TriggerName
        properties = @{
            type           = "ScheduleTrigger"
            typeProperties = @{
                recurrence = @{
                    frequency = "Day"
                    interval  = 1
                    startTime = $ScheduleStartTime.ToString("yyyy-MM-ddTHH:mm:ssZ")
                    timeZone  = (Get-TimeZone).Id
                }
            }
            pipelines = @(
                @{
                    pipelineReference = @{
                        referenceName = $PipelineName
                        type          = "PipelineReference"
                    }
                    parameters = @{
                        days = $days
                    }
                }
            )
        }
    }
    $tmpFile = New-TempAdfJson -Definition $triggerDef
    Set-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Trigger created" -ForegroundColor Green

    # Start the trigger
    Write-Host "Starting trigger..." -ForegroundColor Yellow
    Start-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $TriggerName -Force | Out-Null
    Write-Host "✓ Trigger started" -ForegroundColor Green
    Write-Host "  Schedule: Daily at $($ScheduleStartTime.ToString('HH:mm')) ($((Get-TimeZone).Id))" -ForegroundColor Gray

    # =========================================================================
    # Step 9: Create Service Principal for remote / webhook pipeline triggering
    # =========================================================================
    Write-Host "`n[9] Creating webhook Service Principal" -ForegroundColor Cyan

    $webhookSPName   = "$DataFactoryName-trigger-sp"
    $adfResourceId   = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName"
    $webhookTriggerUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/pipelines/$PipelineName/createRun?api-version=2018-06-01"

    # Check for an existing app with this name
    $existingApp = Get-AzADApplication -DisplayName $webhookSPName -ErrorAction SilentlyContinue | Select-Object -First 1

    if (-not $existingApp) {
        Write-Host "Creating app registration: $webhookSPName..." -ForegroundColor Yellow
        $webhookApp = New-AzADApplication -DisplayName $webhookSPName
    } else {
        Write-Host "App registration already exists: $webhookSPName" -ForegroundColor Yellow
        $webhookApp = $existingApp
    }

    # Ensure a Service Principal exists for the app
    $webhookSP = Get-AzADServicePrincipal -ApplicationId $webhookApp.AppId -ErrorAction SilentlyContinue
    if (-not $webhookSP) {
        Write-Host "Creating service principal..." -ForegroundColor Yellow
        $webhookSP = New-AzADServicePrincipal -ApplicationId $webhookApp.AppId
        Start-Sleep -Seconds 10  # Allow SP to propagate
    }

    # Generate a new client secret (valid 2 years)
    Write-Host "Generating client secret (2-year expiry)..." -ForegroundColor Yellow
    $credResult      = New-AzADAppCredential -ApplicationId $webhookApp.AppId -EndDate (Get-Date).AddYears(2)
    $webhookClientSecret = $credResult.SecretText
    $webhookClientId     = $webhookApp.AppId
    $webhookTenantId     = (Get-AzContext).Tenant.Id

    # Assign Data Factory Contributor on this ADF so the SP can trigger pipeline runs
    $existingSpRole = Get-AzRoleAssignment -ObjectId $webhookSP.Id -RoleDefinitionName "Data Factory Contributor" -Scope $adfResourceId -ErrorAction SilentlyContinue
    if (-not $existingSpRole) {
        $spRoleRetry = 0
        while ($spRoleRetry -lt 5) {
            try {
                New-AzRoleAssignment -ObjectId $webhookSP.Id -RoleDefinitionName "Data Factory Contributor" -Scope $adfResourceId | Out-Null
                Write-Host "✓ Data Factory Contributor role assigned to SP" -ForegroundColor Green
                break
            } catch {
                $spRoleRetry++
                if ($spRoleRetry -ge 5) { throw "Failed to assign SP role after 5 attempts: $_" }
                Write-Host "  Retrying SP role assignment... ($spRoleRetry/5)" -ForegroundColor Yellow
                Start-Sleep -Seconds 5
            }
        }
    } else {
        Write-Host "✓ Data Factory Contributor role already assigned to SP" -ForegroundColor Green
    }

    Write-Host "✓ Webhook SP configured" -ForegroundColor Green
    Write-Host "  SP Name:    $webhookSPName" -ForegroundColor Gray
    Write-Host "  Client ID:  $webhookClientId" -ForegroundColor Gray
    Write-Host "  Tenant ID:  $webhookTenantId" -ForegroundColor Gray
    Write-Host "  Trigger URI: $webhookTriggerUri" -ForegroundColor Gray

    # =========================================================================
    # Summary
    # =========================================================================
    Write-Host "`n$("=" * 80)" -ForegroundColor Green
    Write-Host "SETUP COMPLETE!" -ForegroundColor Green
    Write-Host ("=" * 80) -ForegroundColor Green
    Write-Host "Mode:                 $(if ($usePrivateEndpoint) { 'Managed VNet + Private Endpoint' } else { 'Standard' })" -ForegroundColor White
    Write-Host "Resource Group:       $ResourceGroupName" -ForegroundColor White
    Write-Host "Data Factory:         $DataFactoryName" -ForegroundColor White
    Write-Host "Location:             $location" -ForegroundColor White
    Write-Host "Source:               $exportStorageAccount/$exportStorageContainer/$normalizedPath" -ForegroundColor White
    Write-Host "Destination:          $customerStorageAccount/$destContainerName/$normalizedPath" -ForegroundColor White
    Write-Host "Pipeline:             $PipelineName" -ForegroundColor White
    Write-Host "Trigger:              $TriggerName (Daily at $($ScheduleStartTime.ToString('HH:mm')))" -ForegroundColor White
    Write-Host "Days Filter:          $days" -ForegroundColor White
    if ($usePrivateEndpoint) {
        Write-Host "Integration Runtime:  $IRName (Managed VNet)" -ForegroundColor White
        Write-Host "Private Endpoint:     $ManagedPEName" -ForegroundColor White
    }
    Write-Host "Webhook SP:           $webhookSPName" -ForegroundColor White
    Write-Host "Webhook Tenant ID:    $webhookTenantId" -ForegroundColor White
    Write-Host "Webhook Client ID:    $webhookClientId" -ForegroundColor White
    Write-Host "Webhook Trigger URI:  $webhookTriggerUri" -ForegroundColor White
    Write-Host ("=" * 80) -ForegroundColor Green

    # Webhook usage instructions
    Write-Host "`nTo trigger the pipeline via webhook (PowerShell):" -ForegroundColor Yellow
    Write-Host @"
`$tokenBody = `"grant_type=client_credentials&client_id=$webhookClientId&client_secret=<SECRET>&resource=https://management.azure.com/"
`$tokenResp = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$webhookTenantId/oauth2/token" -Method Post -Body `$tokenBody
Invoke-RestMethod -Uri "$webhookTriggerUri" -Method Post -Headers @{Authorization = "Bearer `$(`$tokenResp.access_token)"} -ContentType "application/json" -Body '{"days": 7}'
"@ -ForegroundColor Gray

    # Manual trigger instructions
    Write-Host "`nTo trigger the pipeline manually:" -ForegroundColor Yellow
    Write-Host @"
Invoke-AzDataFactoryV2Pipeline ``
    -ResourceGroupName "$ResourceGroupName" ``
    -DataFactoryName "$DataFactoryName" ``
    -PipelineName "$PipelineName" ``
    -Parameter @{ days = $days }
"@ -ForegroundColor Gray

    # Build results object
    $results = [PSCustomObject]@{
        Mode               = if ($usePrivateEndpoint) { "ManagedVNet" } else { "Standard" }
        ResourceGroup      = $ResourceGroupName
        DataFactory        = $DataFactoryName
        Location           = $location
        SourceStorage      = $exportStorageAccount
        SourceContainer    = $exportStorageContainer
        SourceFolder       = $normalizedPath
        DestStorage        = $customerStorageAccount
        DestContainer      = $destContainerName
        DestFolder         = $normalizedPath
        Pipeline           = $PipelineName
        Trigger            = $TriggerName
        DaysFilter         = $days
        IntegrationRuntime = if ($usePrivateEndpoint) { $IRName } else { "AutoResolveIntegrationRuntime" }
        PrivateEndpoint    = if ($usePrivateEndpoint) { $ManagedPEName } else { $null }
        ScheduleStartTime  = $ScheduleStartTime
        WebhookSPName      = $webhookSPName
        WebhookTenantId    = $webhookTenantId
        WebhookClientId    = $webhookClientId
        WebhookClientSecret = $webhookClientSecret
        WebhookTriggerUri  = $webhookTriggerUri
    }

    $resultsJson = $results | ConvertTo-Json -Depth 5
    Write-Host "`nReturn Object (JSON):" -ForegroundColor Yellow

    # Upload results to customer storage account
    Write-Host "`nUploading results to customer storage..." -ForegroundColor Cyan
    try {
        $resultsFileName = "$siteName-datafactory.json"
        $containerName = 'runbooks'

        $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken

        # Ensure both required containers exist
        foreach ($requiredContainer in @('runbooks', 'logs')) {
            $existingContainer = Get-AzStorageContainer -Name $requiredContainer -Context $destinationContext -ErrorAction SilentlyContinue
            if (-not $existingContainer) {
                Write-Host "Creating container: $requiredContainer" -ForegroundColor Yellow
                New-AzStorageContainer -Name $requiredContainer -Context $destinationContext -Permission Off | Out-Null
                Write-Host "✓ Container '$requiredContainer' created" -ForegroundColor Green
            } else {
                Write-Host "✓ Container '$requiredContainer' already exists" -ForegroundColor Green
            }
        }

        $tempJsonPath = Join-Path (Get-Location) $resultsFileName
        $resultsJson | Out-File -FilePath $tempJsonPath -Encoding utf8 -Force

        Set-AzStorageBlobContent -File $tempJsonPath -Container $containerName -Blob $resultsFileName -Context $destinationContext -Force | Out-Null
        Write-Host "✓ Results uploaded: $resultsFileName to $customerStorageAccount/$containerName" -ForegroundColor Green

        Remove-Item -Path $tempJsonPath -Force -ErrorAction SilentlyContinue
    }
    catch {
        Write-Host "✗ Failed to upload results: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    return $results

} catch {
    Write-Host "`n❌ Error occurred: $_" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    throw
}
