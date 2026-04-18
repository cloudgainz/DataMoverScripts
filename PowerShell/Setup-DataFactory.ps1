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

# Folder names under exportsDirectory for the three data classes
$dailiesFolder   = $parameterTable.dailiesFolder
$monthliesFolder = $parameterTable.monthliesFolder
$onetimeFolder   = $parameterTable.onetimeFolder

# Scheduling — all run times are HH:mm, interpreted in $timeZone
$timeZone            = if ($parameterTable.timeZone)            { $parameterTable.timeZone }            else { 'Eastern Standard Time' }
$dailyRunTime        = if ($parameterTable.dailyRunTime)        { $parameterTable.dailyRunTime }        else { '02:00' }
$catchupRunTime      = if ($parameterTable.catchupRunTime)      { $parameterTable.catchupRunTime }      else { '02:30' }
$monthlyRunTime      = if ($parameterTable.monthlyRunTime)      { $parameterTable.monthlyRunTime }      else { '03:00' }
$monthlyDayOfMonth   = if ($parameterTable.monthlyDayOfMonth)   { [int]$parameterTable.monthlyDayOfMonth }   else { 6 }
$catchupCutoffDay    = if ($parameterTable.catchupCutoffDay)    { [int]$parameterTable.catchupCutoffDay }    else { 5 }
$dailyLookbackDays   = if ($parameterTable.dailyLookbackDays)   { [int]$parameterTable.dailyLookbackDays }   else { 2 }
$catchupLookbackDays = if ($parameterTable.catchupLookbackDays) { [int]$parameterTable.catchupLookbackDays } else { 7 }

if ($catchupCutoffDay -lt 1 -or $catchupCutoffDay -gt 28) {
    throw "parameterTable.catchupCutoffDay ($catchupCutoffDay) must be between 1 and 28"
}

# Validate required folder names
foreach ($pair in @(
    @{ Name = 'dailiesFolder';   Value = $dailiesFolder },
    @{ Name = 'monthliesFolder'; Value = $monthliesFolder },
    @{ Name = 'onetimeFolder';   Value = $onetimeFolder }
)) {
    if ([string]::IsNullOrWhiteSpace($pair.Value)) {
        throw "parameterTable.$($pair.Name) is required"
    }
}

Set-AzContext -Subscription $subscriptionName

# Naming conventions
[string]$DataFactoryName         = $siteName + "-adf"
[string]$PipelineName            = $siteName + "-DataMoverPipeline"
[string]$DailyTriggerName        = $siteName + "-DailyTrigger"
[string]$CatchupTriggerName      = $siteName + "-CatchupTrigger"
[string]$MonthlyTriggerName      = $siteName + "-MonthlyTrigger"
[string]$SourceLinkedServiceName = "SourceStorage"
[string]$DestLinkedServiceName   = "DestStorage"
[string]$SourceDatasetName       = "SourceBlobs"
[string]$DestDatasetName         = "DestBlobs"
[string]$IRName                  = $siteName + "-ManagedIR"
[string]$ManagedPEName           = $siteName + "-SourceStoragePE"
[hashtable]$Tags = @{}

$normalizedPath = $exportsDirectory.Trim('/')
$destContainerName = $siteName.ToLower()
$usePrivateEndpoint = -not [string]::IsNullOrWhiteSpace($vnetSubnetId)

# Trigger startTime — slightly in the past so the next scheduled occurrence fires
[DateTime]$ScheduleStartTime = (Get-Date).AddMinutes(-5)

# Parse HH:mm run-time strings into hour/minute pairs for trigger schedules
function ConvertTo-RunTimeParts {
    param([Parameter(Mandatory)] [string]$TimeString)
    if ($TimeString -notmatch '^\d{1,2}:\d{2}$') {
        throw "Invalid run time '$TimeString' — expected HH:mm (e.g. '02:00')"
    }
    $parts = $TimeString.Split(':')
    return @{ Hour = [int]$parts[0]; Minute = [int]$parts[1] }
}
$dailyTime   = ConvertTo-RunTimeParts -TimeString $dailyRunTime
$catchupTime = ConvertTo-RunTimeParts -TimeString $catchupRunTime
$monthlyTime = ConvertTo-RunTimeParts -TimeString $monthlyRunTime

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

    # Source dataset — Binary, parameterized on sourceFolder + dateRange.
    # Pipeline supplies these per run; full path = {normalizedPath}/{sourceFolder}/{dateRange}
    Write-Host "Creating source dataset: $SourceDatasetName..." -ForegroundColor Yellow
    $sourceDatasetDef = @{
        name       = $SourceDatasetName
        properties = @{
            type              = "Binary"
            linkedServiceName = @{
                referenceName = $SourceLinkedServiceName
                type          = "LinkedServiceReference"
            }
            parameters = @{
                sourceFolder = @{ type = "String" }
                dateRange    = @{ type = "String" }
            }
            typeProperties = @{
                location = @{
                    type      = "AzureBlobStorageLocation"
                    container = $exportStorageContainer
                    folderPath = @{
                        value = "@concat('$normalizedPath', '/', dataset().sourceFolder, '/', dataset().dateRange)"
                        type  = "Expression"
                    }
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $sourceDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $SourceDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Source dataset created (parameterized: $exportStorageContainer/$normalizedPath/{sourceFolder}/{dateRange})" -ForegroundColor Green

    # Destination dataset — Binary, mirrors {sourceFolder}/{dateRange} on the customer side
    Write-Host "Creating destination dataset: $DestDatasetName..." -ForegroundColor Yellow
    $destDatasetDef = @{
        name       = $DestDatasetName
        properties = @{
            type              = "Binary"
            linkedServiceName = @{
                referenceName = $DestLinkedServiceName
                type          = "LinkedServiceReference"
            }
            parameters = @{
                sourceFolder = @{ type = "String" }
                dateRange    = @{ type = "String" }
            }
            typeProperties = @{
                location = @{
                    type      = "AzureBlobStorageLocation"
                    container = $destContainerName
                    folderPath = @{
                        value = "@concat('$normalizedPath', '/', dataset().sourceFolder, '/', dataset().dateRange)"
                        type  = "Expression"
                    }
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $destDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $DestDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Destination dataset created (parameterized: $destContainerName/$normalizedPath/{sourceFolder}/{dateRange})" -ForegroundColor Green

    # =========================================================================
    # Step 7: Create Pipeline
    # =========================================================================
    Write-Host "`n[7] Creating Pipeline: $PipelineName" -ForegroundColor Cyan

    # Pipeline parameters (supplied by triggers / on-demand caller):
    #   sourceFolderName  — subfolder of $normalizedPath to copy from (dailies/monthly/onetime)
    #   monthOffset       — 0 = current month folder, -1 = previous month folder
    #   lookbackDays      — LastModified filter window in days (0 = no filter, grab all)
    #   mode              — label for log/audit (daily/catchup/monthly/onetime)
    #
    # Pipeline computes dateRange ('yyyyMMdd-yyyyMMdd') at run time from monthOffset
    # in $timeZone, then copies {sourceFolderName}/{dateRange}/** to the same path
    # on the customer side. Run summary written to logs/{siteName}-{mode}-{RunId}.json
    $logBlobUrlExpr = "@concat('https://$customerStorageAccount.blob.core.windows.net/logs/$siteName-', pipeline().parameters.mode, '-', pipeline().RunId, '.json?$customerToken')"

    # ADF expressions kept as PowerShell variables for readability
    $targetDateExpr = "@if(equals(pipeline().parameters.monthOffset, 0), convertTimeZone(utcnow(), 'UTC', '$timeZone'), subtractFromTime(convertTimeZone(utcnow(), 'UTC', '$timeZone'), 1, 'Month'))"
    $dateRangeExpr  = "@concat(formatDateTime(startOfMonth(variables('targetDate')), 'yyyyMMdd'), '-', formatDateTime(addDays(addToTime(startOfMonth(variables('targetDate')), 1, 'Month'), -1), 'yyyyMMdd'))"
    $modifiedFilterExpr = "@if(greater(pipeline().parameters.lookbackDays, 0), formatDateTime(addDays(utcnow(), mul(pipeline().parameters.lookbackDays, -1)), 'yyyy-MM-ddTHH:mm:ssZ'), '1900-01-01T00:00:00Z')"

    $successBody = @{
        runId               = "@{pipeline().RunId}"
        pipeline            = "@{pipeline().Pipeline}"
        mode                = "@{pipeline().parameters.mode}"
        sourceFolder        = "@{pipeline().parameters.sourceFolderName}"
        dateRange           = "@{variables('dateRange')}"
        lookbackDays        = "@{pipeline().parameters.lookbackDays}"
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
        mode         = "@{pipeline().parameters.mode}"
        sourceFolder = "@{pipeline().parameters.sourceFolderName}"
        dateRange    = "@{variables('dateRange')}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "Failed"
        errorCode    = "@{activity('CopyBlobs').error.errorCode}"
        errorMessage = "@{activity('CopyBlobs').error.message}"
    }

    # Body for when the logging activity itself errors (CopyBlobs succeeded but WriteSuccessLog failed)
    $logErrorBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        mode         = "@{pipeline().parameters.mode}"
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
                sourceFolderName = @{ type = "String" }
                monthOffset      = @{ type = "Int";    defaultValue = 0 }
                lookbackDays     = @{ type = "Int";    defaultValue = $dailyLookbackDays }
                mode             = @{ type = "String"; defaultValue = "daily" }
            }
            variables = @{
                targetDate = @{ type = "String" }
                dateRange  = @{ type = "String" }
            }
            activities = @(
                # ------------------------------------------------------------------
                # SetTargetDate: compute timestamp for current or prior month
                # ------------------------------------------------------------------
                @{
                    name = "SetTargetDate"
                    type = "SetVariable"
                    typeProperties = @{
                        variableName = "targetDate"
                        value        = @{ value = $targetDateExpr; type = "Expression" }
                    }
                },
                # ------------------------------------------------------------------
                # SetDateRange: format yyyyMMdd-yyyyMMdd for the target month
                # ------------------------------------------------------------------
                @{
                    name      = "SetDateRange"
                    type      = "SetVariable"
                    dependsOn = @(
                        @{ activity = "SetTargetDate"; dependencyConditions = @("Succeeded") }
                    )
                    typeProperties = @{
                        variableName = "dateRange"
                        value        = @{ value = $dateRangeExpr; type = "Expression" }
                    }
                },
                # ------------------------------------------------------------------
                # CopyBlobs: copy {sourceFolder}/{dateRange}/** to mirrored path
                # ------------------------------------------------------------------
                @{
                    name      = "CopyBlobs"
                    type      = "Copy"
                    dependsOn = @(
                        @{ activity = "SetDateRange"; dependencyConditions = @("Succeeded") }
                    )
                    inputs  = @(
                        @{
                            referenceName = $SourceDatasetName
                            type          = "DatasetReference"
                            parameters = @{
                                sourceFolder = "@pipeline().parameters.sourceFolderName"
                                dateRange    = "@variables('dateRange')"
                            }
                        }
                    )
                    outputs = @(
                        @{
                            referenceName = $DestDatasetName
                            type          = "DatasetReference"
                            parameters = @{
                                sourceFolder = "@pipeline().parameters.sourceFolderName"
                                dateRange    = "@variables('dateRange')"
                            }
                        }
                    )
                    typeProperties = @{
                        source = @{
                            type          = "BinarySource"
                            storeSettings = @{
                                type                       = "AzureBlobStorageReadSettings"
                                recursive                  = $true
                                modifiedDatetimeStart      = @{
                                    value = $modifiedFilterExpr
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
    Write-Host "✓ Pipeline created (mode-driven; copies {sourceFolderName}/{computed-dateRange}/**)" -ForegroundColor Green

    # =========================================================================
    # Step 8: Create and Start Triggers (Daily, Catchup, Monthly)
    # On-demand "onetime" runs are webhook-only — no scheduled trigger.
    # =========================================================================
    Write-Host "`n[8] Creating Triggers" -ForegroundColor Cyan

    function Stop-AdfTriggerIfRunning {
        param([Parameter(Mandatory)] [string]$Name)
        $existing = Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $Name -ErrorAction SilentlyContinue
        if ($existing -and $existing.RuntimeState -eq "Started") {
            Write-Host "  Stopping existing trigger '$Name'..." -ForegroundColor Yellow
            Stop-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $Name -Force | Out-Null
        }
    }

    function New-AdfTriggerDefinition {
        param(
            [Parameter(Mandatory)] [string]$Name,
            [Parameter(Mandatory)] [hashtable]$Recurrence,
            [Parameter(Mandatory)] [hashtable]$PipelineParameters
        )
        return @{
            name       = $Name
            properties = @{
                type           = "ScheduleTrigger"
                typeProperties = @{ recurrence = $Recurrence }
                pipelines = @(
                    @{
                        pipelineReference = @{
                            referenceName = $PipelineName
                            type          = "PipelineReference"
                        }
                        parameters = $PipelineParameters
                    }
                )
            }
        }
    }

    function Set-AdfTrigger {
        param(
            [Parameter(Mandatory)] [string]$Name,
            [Parameter(Mandatory)] [hashtable]$Definition
        )
        Stop-AdfTriggerIfRunning -Name $Name
        $tmpFile = New-TempAdfJson -Definition $Definition
        Set-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $Name -DefinitionFile $tmpFile -Force | Out-Null
        Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
        Start-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $Name -Force | Out-Null
        Write-Host "✓ Trigger '$Name' created + started" -ForegroundColor Green
    }

    $startTimeIso = $ScheduleStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    # ---- DailyTrigger: every day at $dailyRunTime $timeZone (current month dailies) ----
    Write-Host "DailyTrigger — every day $dailyRunTime $timeZone (current month dailies, lookback $dailyLookbackDays d)" -ForegroundColor Yellow
    $dailyRecurrence = @{
        frequency = "Day"
        interval  = 1
        startTime = $startTimeIso
        timeZone  = $timeZone
        schedule  = @{
            hours   = @($dailyTime.Hour)
            minutes = @($dailyTime.Minute)
        }
    }
    $dailyParams = @{
        sourceFolderName = $dailiesFolder
        monthOffset      = 0
        lookbackDays     = $dailyLookbackDays
        mode             = "daily"
    }
    Set-AdfTrigger -Name $DailyTriggerName -Definition (New-AdfTriggerDefinition -Name $DailyTriggerName -Recurrence $dailyRecurrence -PipelineParameters $dailyParams)

    # ---- CatchupTrigger: days 1..catchupCutoffDay at $catchupRunTime $timeZone (prior month dailies) ----
    $catchupDays = @(1..$catchupCutoffDay)
    Write-Host "CatchupTrigger — days 1-$catchupCutoffDay at $catchupRunTime $timeZone (prior month dailies, lookback $catchupLookbackDays d)" -ForegroundColor Yellow
    $catchupRecurrence = @{
        frequency = "Month"
        interval  = 1
        startTime = $startTimeIso
        timeZone  = $timeZone
        schedule  = @{
            monthDays = $catchupDays
            hours     = @($catchupTime.Hour)
            minutes   = @($catchupTime.Minute)
        }
    }
    $catchupParams = @{
        sourceFolderName = $dailiesFolder
        monthOffset      = -1
        lookbackDays     = $catchupLookbackDays
        mode             = "catchup"
    }
    Set-AdfTrigger -Name $CatchupTriggerName -Definition (New-AdfTriggerDefinition -Name $CatchupTriggerName -Recurrence $catchupRecurrence -PipelineParameters $catchupParams)

    # ---- MonthlyTrigger: day $monthlyDayOfMonth at $monthlyRunTime $timeZone (prior month monthlies) ----
    Write-Host "MonthlyTrigger — day $monthlyDayOfMonth at $monthlyRunTime $timeZone (prior month monthlies, no lookback filter)" -ForegroundColor Yellow
    $monthlyRecurrence = @{
        frequency = "Month"
        interval  = 1
        startTime = $startTimeIso
        timeZone  = $timeZone
        schedule  = @{
            monthDays = @($monthlyDayOfMonth)
            hours     = @($monthlyTime.Hour)
            minutes   = @($monthlyTime.Minute)
        }
    }
    $monthlyParams = @{
        sourceFolderName = $monthliesFolder
        monthOffset      = -1
        lookbackDays     = 0
        mode             = "monthly"
    }
    Set-AdfTrigger -Name $MonthlyTriggerName -Definition (New-AdfTriggerDefinition -Name $MonthlyTriggerName -Recurrence $monthlyRecurrence -PipelineParameters $monthlyParams)

    Write-Host "✓ Schedule triggers created. Onetime runs are invoked via webhook only." -ForegroundColor Green

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
    Write-Host "  Dailies folder:     $dailiesFolder" -ForegroundColor White
    Write-Host "  Monthlies folder:   $monthliesFolder" -ForegroundColor White
    Write-Host "  Onetime folder:     $onetimeFolder" -ForegroundColor White
    Write-Host "Destination:          $customerStorageAccount/$destContainerName/$normalizedPath" -ForegroundColor White
    Write-Host "Pipeline:             $PipelineName" -ForegroundColor White
    Write-Host "TimeZone:             $timeZone" -ForegroundColor White
    Write-Host "Triggers:" -ForegroundColor White
    Write-Host "  $DailyTriggerName    — daily $dailyRunTime (lookback $dailyLookbackDays d)" -ForegroundColor White
    Write-Host "  $CatchupTriggerName  — days 1-$catchupCutoffDay at $catchupRunTime (lookback $catchupLookbackDays d)" -ForegroundColor White
    Write-Host "  $MonthlyTriggerName  — day $monthlyDayOfMonth at $monthlyRunTime (no lookback filter)" -ForegroundColor White
    Write-Host "  (onetime: webhook-only — no scheduled trigger)" -ForegroundColor White
    if ($usePrivateEndpoint) {
        Write-Host "Integration Runtime:  $IRName (Managed VNet)" -ForegroundColor White
        Write-Host "Private Endpoint:     $ManagedPEName" -ForegroundColor White
    }
    Write-Host "Webhook SP:           $webhookSPName" -ForegroundColor White
    Write-Host "Webhook Tenant ID:    $webhookTenantId" -ForegroundColor White
    Write-Host "Webhook Client ID:    $webhookClientId" -ForegroundColor White
    Write-Host "Webhook Trigger URI:  $webhookTriggerUri" -ForegroundColor White
    Write-Host ("=" * 80) -ForegroundColor Green

    # Webhook usage instructions — onetime / on-demand
    Write-Host "`nTo trigger an on-demand 'onetime' run via webhook (PowerShell):" -ForegroundColor Yellow
    Write-Host @"
`$tokenBody = `"grant_type=client_credentials&client_id=$webhookClientId&client_secret=<SECRET>&resource=https://management.azure.com/"
`$tokenResp = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$webhookTenantId/oauth2/token" -Method Post -Body `$tokenBody
`$body = @{
    sourceFolderName = '$onetimeFolder'
    monthOffset      = 0       # 0 = current month folder, -1 = prior month
    lookbackDays     = 0       # 0 = no LastModified filter (grab everything in folder)
    mode             = 'onetime'
} | ConvertTo-Json
Invoke-RestMethod -Uri "$webhookTriggerUri" -Method Post -Headers @{Authorization = "Bearer `$(`$tokenResp.access_token)"} -ContentType "application/json" -Body `$body
"@ -ForegroundColor Gray

    # Manual trigger instructions
    Write-Host "`nTo trigger the pipeline manually (PowerShell):" -ForegroundColor Yellow
    Write-Host @"
Invoke-AzDataFactoryV2Pipeline ``
    -ResourceGroupName "$ResourceGroupName" ``
    -DataFactoryName "$DataFactoryName" ``
    -PipelineName "$PipelineName" ``
    -Parameter @{ sourceFolderName = '$onetimeFolder'; monthOffset = 0; lookbackDays = 0; mode = 'onetime' }
"@ -ForegroundColor Gray

    # Build results object (Add-Member style per project convention)
    $results = New-Object psobject
    $results | Add-Member -MemberType NoteProperty -Name Mode                -Value $(if ($usePrivateEndpoint) { "ManagedVNet" } else { "Standard" })
    $results | Add-Member -MemberType NoteProperty -Name ResourceGroup       -Value $ResourceGroupName
    $results | Add-Member -MemberType NoteProperty -Name DataFactory         -Value $DataFactoryName
    $results | Add-Member -MemberType NoteProperty -Name Location            -Value $location
    $results | Add-Member -MemberType NoteProperty -Name SourceStorage       -Value $exportStorageAccount
    $results | Add-Member -MemberType NoteProperty -Name SourceContainer     -Value $exportStorageContainer
    $results | Add-Member -MemberType NoteProperty -Name SourceFolder        -Value $normalizedPath
    $results | Add-Member -MemberType NoteProperty -Name DailiesFolder       -Value $dailiesFolder
    $results | Add-Member -MemberType NoteProperty -Name MonthliesFolder     -Value $monthliesFolder
    $results | Add-Member -MemberType NoteProperty -Name OnetimeFolder       -Value $onetimeFolder
    $results | Add-Member -MemberType NoteProperty -Name DestStorage         -Value $customerStorageAccount
    $results | Add-Member -MemberType NoteProperty -Name DestContainer       -Value $destContainerName
    $results | Add-Member -MemberType NoteProperty -Name DestFolder          -Value $normalizedPath
    $results | Add-Member -MemberType NoteProperty -Name Pipeline            -Value $PipelineName
    $results | Add-Member -MemberType NoteProperty -Name TimeZone            -Value $timeZone
    $results | Add-Member -MemberType NoteProperty -Name DailyTrigger        -Value $DailyTriggerName
    $results | Add-Member -MemberType NoteProperty -Name DailyRunTime        -Value $dailyRunTime
    $results | Add-Member -MemberType NoteProperty -Name DailyLookbackDays   -Value $dailyLookbackDays
    $results | Add-Member -MemberType NoteProperty -Name CatchupTrigger      -Value $CatchupTriggerName
    $results | Add-Member -MemberType NoteProperty -Name CatchupRunTime      -Value $catchupRunTime
    $results | Add-Member -MemberType NoteProperty -Name CatchupCutoffDay    -Value $catchupCutoffDay
    $results | Add-Member -MemberType NoteProperty -Name CatchupLookbackDays -Value $catchupLookbackDays
    $results | Add-Member -MemberType NoteProperty -Name MonthlyTrigger      -Value $MonthlyTriggerName
    $results | Add-Member -MemberType NoteProperty -Name MonthlyDayOfMonth   -Value $monthlyDayOfMonth
    $results | Add-Member -MemberType NoteProperty -Name MonthlyRunTime      -Value $monthlyRunTime
    $results | Add-Member -MemberType NoteProperty -Name IntegrationRuntime  -Value $(if ($usePrivateEndpoint) { $IRName } else { "AutoResolveIntegrationRuntime" })
    $results | Add-Member -MemberType NoteProperty -Name PrivateEndpoint     -Value $(if ($usePrivateEndpoint) { $ManagedPEName } else { $null })
    $results | Add-Member -MemberType NoteProperty -Name ScheduleStartTime   -Value $ScheduleStartTime
    $results | Add-Member -MemberType NoteProperty -Name WebhookSPName       -Value $webhookSPName
    $results | Add-Member -MemberType NoteProperty -Name WebhookTenantId     -Value $webhookTenantId
    $results | Add-Member -MemberType NoteProperty -Name WebhookClientId     -Value $webhookClientId
    $results | Add-Member -MemberType NoteProperty -Name WebhookClientSecret -Value $webhookClientSecret
    $results | Add-Member -MemberType NoteProperty -Name WebhookTriggerUri   -Value $webhookTriggerUri

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
