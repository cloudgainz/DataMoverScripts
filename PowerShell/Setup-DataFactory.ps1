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

# Folder names under exportsDirectory — daily and monthly cadence outputs
$dailiesFolder   = $parameterTable.dailiesFolder
$monthliesFolder = $parameterTable.monthliesFolder

# Scheduling — all run times are HH:mm, interpreted in $timeZone.
# catchupCutoffDay is the integer N used in (today - N) to compute which
# month folder to mirror. With N=5, runs on May 1-5 still target April; on
# May 6+ they target May. Same calculation drives both Daily and Monthly.
$timeZone          = if ($parameterTable.timeZone)          { $parameterTable.timeZone }          else { 'Eastern Standard Time' }
$dailyRunTime      = if ($parameterTable.dailyRunTime)      { $parameterTable.dailyRunTime }      else { '02:00' }
$monthlyRunTime    = if ($parameterTable.monthlyRunTime)    { $parameterTable.monthlyRunTime }    else { '03:00' }
$monthlyDayOfMonth = if ($parameterTable.monthlyDayOfMonth) { [int]$parameterTable.monthlyDayOfMonth } else { 6 }
$catchupCutoffDay  = if ($parameterTable.catchupCutoffDay)  { [int]$parameterTable.catchupCutoffDay }  else { 5 }

if ($catchupCutoffDay -lt 1 -or $catchupCutoffDay -gt 28) {
    throw "parameterTable.catchupCutoffDay ($catchupCutoffDay) must be between 1 and 28"
}

# Validate required folder names
foreach ($pair in @(
    @{ Name = 'dailiesFolder';   Value = $dailiesFolder },
    @{ Name = 'monthliesFolder'; Value = $monthliesFolder }
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
[string]$MonthlyTriggerName      = $siteName + "-MonthlyTrigger"
[string]$LegacyCatchupName       = $siteName + "-CatchupTrigger"  # for cleanup of pre-mirror deployments
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

# Resolve a writable temp dir cross-platform (Windows uses $env:TEMP, Linux/Cloud Shell uses /tmp).
# Cloud Shell on Linux does not set $env:TEMP, so fall back to [IO.Path]::GetTempPath() and
# create it if missing.
$tempDir = [System.IO.Path]::GetTempPath()
if ([string]::IsNullOrWhiteSpace($tempDir)) {
    $tempDir = if ($IsWindows) { 'C:\Windows\Temp' } else { '/tmp' }
}
if (-not (Test-Path -LiteralPath $tempDir)) {
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
}
Write-Host "Using temp dir: $tempDir" -ForegroundColor Gray

# Helper: create a temp JSON file from a hashtable (for ADF cmdlet -DefinitionFile params)
function New-TempAdfJson {
    param([hashtable]$Definition)
    $tempFile = Join-Path $tempDir "adf_$(New-Guid).json"
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

    # Source/Dest datasets — Binary, parameterized by folderPath.
    # Pipeline drives folderPath per-guid so each Copy targets a single guid directory,
    # which sidesteps the HNS directory-placeholder blobs that live at parent levels.
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
                folderPath = @{ type = "String" }
            }
            typeProperties = @{
                location = @{
                    type       = "AzureBlobStorageLocation"
                    container  = $exportStorageContainer
                    folderPath = @{ value = "@dataset().folderPath"; type = "Expression" }
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $sourceDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $SourceDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Source dataset created (parameterized: $exportStorageContainer/{folderPath})" -ForegroundColor Green

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
                folderPath = @{ type = "String" }
            }
            typeProperties = @{
                location = @{
                    type       = "AzureBlobStorageLocation"
                    container  = $destContainerName
                    folderPath = @{ value = "@dataset().folderPath"; type = "Expression" }
                }
            }
        }
    }
    $tmpFile = New-TempAdfJson -Definition $destDatasetDef
    Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $DestDatasetName -DefinitionFile $tmpFile -Force | Out-Null
    Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue
    Write-Host "✓ Destination dataset created (parameterized: $destContainerName/{folderPath})" -ForegroundColor Green

    # =========================================================================
    # Step 7: Create Pipeline
    # =========================================================================
    Write-Host "`n[7] Creating Pipeline: $PipelineName" -ForegroundColor Cyan

    # Pipeline parameters (supplied by triggers / on-demand caller):
    #   sourceFolderName  — subfolder of $normalizedPath to mirror (dailies / monthlies)
    #   catchupDays       — integer N; pipeline mirrors the month folder for (today - N).
    #                       For first N days of any new month this lands in the prior month;
    #                       afterwards it's the current month. Same value drives Daily and Monthly.
    #   mode              — label for log/audit (daily / monthly)
    #
    # Pipeline computes dateRange ('yyyyMMdd-yyyyMMdd') from (today - catchupDays) in
    # $timeZone, then mirrors {sourceFolderName}/{dateRange}/** to the same path on
    # the customer side: Delete the destination folder entirely, then Copy the source
    # folder as-is (recursive, no date filter). Run summary written to
    # logs/{siteName}-{mode}-{RunId}.json
    $logBlobUrlExpr = "@concat('https://$customerStorageAccount.blob.core.windows.net/logs/$siteName-', pipeline().parameters.mode, '-', pipeline().RunId, '.json?$customerToken')"

    # ADF expressions kept as PowerShell variables for readability
    $targetDateExpr = "@addDays(convertTimeZone(utcnow(), 'UTC', '$timeZone'), mul(-1, pipeline().parameters.catchupDays))"
    $dateRangeExpr  = "@concat(formatDateTime(startOfMonth(variables('targetDate')), 'yyyyMMdd'), '-', formatDateTime(addDays(addToTime(startOfMonth(variables('targetDate')), 1, 'Month'), -1), 'yyyyMMdd'))"

    $successBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        mode         = "@{pipeline().parameters.mode}"
        sourceFolder = "@{pipeline().parameters.sourceFolderName}"
        dateRange    = "@{variables('dateRange')}"
        catchupDays  = "@{pipeline().parameters.catchupDays}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "Succeeded"
        filesRead           = "@{activity('CopyMonthFolder').output.filesRead}"
        filesWritten        = "@{activity('CopyMonthFolder').output.filesWritten}"
        dataReadBytes       = "@{activity('CopyMonthFolder').output.dataRead}"
        dataWrittenBytes    = "@{activity('CopyMonthFolder').output.dataWritten}"
        copyDurationSeconds = "@{activity('CopyMonthFolder').output.copyDuration}"
        throughputMBps      = "@{if(greater(int(activity('CopyMonthFolder').output.copyDuration), 0), div(div(int(activity('CopyMonthFolder').output.dataWritten), 1048576), int(activity('CopyMonthFolder').output.copyDuration)), 0)}"
    }

    $failureBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        mode         = "@{pipeline().parameters.mode}"
        sourceFolder = "@{pipeline().parameters.sourceFolderName}"
        dateRange    = "@{variables('dateRange')}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "Failed"
        errorCode    = "@{coalesce(activity('DeleteDestFolder').error.errorCode, activity('CopyMonthFolder').error.errorCode)}"
        errorMessage = "@{coalesce(activity('DeleteDestFolder').error.message, activity('CopyMonthFolder').error.message)}"
    }

    # Body for when the logging activity itself errors (copy succeeded but WriteSuccessLog failed)
    $logErrorBody = @{
        runId        = "@{pipeline().RunId}"
        pipeline     = "@{pipeline().Pipeline}"
        mode         = "@{pipeline().parameters.mode}"
        triggerTime  = "@{string(pipeline().TriggerTime)}"
        status       = "LoggingError"
        errorCode    = "@{activity('WriteSuccessLog').error.errorCode}"
        errorMessage = "@{activity('WriteSuccessLog').error.message}"
        note         = "Copy succeeded but the success log write failed"
    }

    $pipelineDef = @{
        name       = $PipelineName
        properties = @{
            parameters = @{
                sourceFolderName = @{ type = "String" }
                catchupDays      = @{ type = "Int";    defaultValue = $catchupCutoffDay }
                mode             = @{ type = "String"; defaultValue = "daily" }
            }
            variables = @{
                targetDate      = @{ type = "String" }
                dateRange       = @{ type = "String" }
                dateRangeFolder = @{ type = "String" }
            }
            activities = @(
                # SetTargetDate — anchor date for which month folder to mirror
                @{
                    name = "SetTargetDate"
                    type = "SetVariable"
                    typeProperties = @{
                        variableName = "targetDate"
                        value        = @{ value = $targetDateExpr; type = "Expression" }
                    }
                },
                # SetDateRange — yyyyMMdd-yyyyMMdd for the target month
                @{
                    name      = "SetDateRange"
                    type      = "SetVariable"
                    dependsOn = @( @{ activity = "SetTargetDate"; dependencyConditions = @("Succeeded") } )
                    typeProperties = @{
                        variableName = "dateRange"
                        value        = @{ value = $dateRangeExpr; type = "Expression" }
                    }
                },
                # SetDateRangeFolder — full prefix {exportsDir}/{sourceFolder}/{dateRange}
                @{
                    name      = "SetDateRangeFolder"
                    type      = "SetVariable"
                    dependsOn = @( @{ activity = "SetDateRange"; dependencyConditions = @("Succeeded") } )
                    typeProperties = @{
                        variableName = "dateRangeFolder"
                        value = @{
                            value = "@concat('$normalizedPath', '/', pipeline().parameters.sourceFolderName, '/', variables('dateRange'))"
                            type  = "Expression"
                        }
                    }
                },
                # DeleteDestFolder — wipe the destination month folder before the copy so
                # the dest mirrors the source exactly. Idempotent: if the folder doesn't
                # exist yet, Delete reports 0 deleted and continues.
                @{
                    name      = "DeleteDestFolder"
                    type      = "Delete"
                    dependsOn = @( @{ activity = "SetDateRangeFolder"; dependencyConditions = @("Succeeded") } )
                    typeProperties = @{
                        dataset = @{
                            referenceName = $DestDatasetName
                            type          = "DatasetReference"
                            parameters = @{
                                folderPath = @{ value = "@variables('dateRangeFolder')"; type = "Expression" }
                            }
                        }
                        enableLogging = $false
                        storeSettings = @{ type = "AzureBlobStorageReadSettings"; recursive = $true }
                    }
                },
                # CopyMonthFolder — copy the entire source month folder as-is. Recursive,
                # no date filter, no per-guid iteration. Single activity, single output.
                @{
                    name      = "CopyMonthFolder"
                    type      = "Copy"
                    dependsOn = @( @{ activity = "DeleteDestFolder"; dependencyConditions = @("Succeeded") } )
                    inputs = @(
                        @{
                            referenceName = $SourceDatasetName
                            type          = "DatasetReference"
                            parameters = @{
                                folderPath = @{ value = "@variables('dateRangeFolder')"; type = "Expression" }
                            }
                        }
                    )
                    outputs = @(
                        @{
                            referenceName = $DestDatasetName
                            type          = "DatasetReference"
                            parameters = @{
                                folderPath = @{ value = "@variables('dateRangeFolder')"; type = "Expression" }
                            }
                        }
                    )
                    typeProperties = @{
                        source = @{
                            type          = "BinarySource"
                            storeSettings = @{
                                type      = "AzureBlobStorageReadSettings"
                                recursive = $true
                            }
                        }
                        sink = @{
                            type          = "BinarySink"
                            storeSettings = @{ type = "AzureBlobStorageWriteSettings" }
                        }
                    }
                },
                # WriteSuccessLog — PUT a JSON summary blob on success
                @{
                    name      = "WriteSuccessLog"
                    type      = "WebActivity"
                    dependsOn = @(
                        @{ activity = "CopyMonthFolder"; dependencyConditions = @("Succeeded") }
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
                # WriteFailureLog: PUT a JSON error summary blob if Delete OR Copy failed
                @{
                    name      = "WriteFailureLog"
                    type      = "WebActivity"
                    dependsOn = @(
                        @{ activity = "DeleteDestFolder"; dependencyConditions = @("Failed") },
                        @{ activity = "CopyMonthFolder";  dependencyConditions = @("Failed") }
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
                # WriteLogError: fallback if WriteSuccessLog itself fails
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
    Write-Host "✓ Pipeline created (delete + copy whole month folder; target = today - catchupDays)" -ForegroundColor Green

    # =========================================================================
    # Step 8: Create and Start Triggers (Daily + Monthly)
    #     Daily fires every day; Monthly fires on monthlyDayOfMonth. Both pass
    #     the same catchupDays so the pipeline computes the same target month.
    #     On-demand runs (from the customer portal) invoke the same pipeline
    #     with daily params via the webhook in Step 9.
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

    # ---- Drop the legacy Catchup trigger if a previous deploy of this site left
    #      one behind. The catchup window is now folded into Daily via catchupDays. ----
    $legacyCatchup = Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $LegacyCatchupName -ErrorAction SilentlyContinue
    if ($legacyCatchup) {
        Write-Host "Removing legacy Catchup trigger '$LegacyCatchupName'..." -ForegroundColor Yellow
        Stop-AdfTriggerIfRunning -Name $LegacyCatchupName
        Remove-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $LegacyCatchupName -Force -ErrorAction SilentlyContinue | Out-Null
        Write-Host "✓ Legacy Catchup trigger removed" -ForegroundColor Green
    }

    # ---- DailyTrigger: every day at $dailyRunTime $timeZone ----
    Write-Host "DailyTrigger — every day $dailyRunTime $timeZone (target month = today - $catchupCutoffDay d)" -ForegroundColor Yellow
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
        catchupDays      = $catchupCutoffDay
        mode             = "daily"
    }
    Set-AdfTrigger -Name $DailyTriggerName -Definition (New-AdfTriggerDefinition -Name $DailyTriggerName -Recurrence $dailyRecurrence -PipelineParameters $dailyParams)

    # ---- MonthlyTrigger: day $monthlyDayOfMonth at $monthlyRunTime $timeZone ----
    Write-Host "MonthlyTrigger — day $monthlyDayOfMonth at $monthlyRunTime $timeZone (target month = today - $catchupCutoffDay d)" -ForegroundColor Yellow
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
        catchupDays      = $catchupCutoffDay
        mode             = "monthly"
    }
    Set-AdfTrigger -Name $MonthlyTriggerName -Definition (New-AdfTriggerDefinition -Name $MonthlyTriggerName -Recurrence $monthlyRecurrence -PipelineParameters $monthlyParams)

    Write-Host "✓ Schedule triggers created. On-demand runs invoke the daily pipeline via webhook." -ForegroundColor Green

    # =========================================================================
    # Step 9: Create Service Principal for on-demand pipeline triggering
    #
    # This SP is used ONLY to allow the customer portal to fire on-demand
    # pipeline runs via the ADF createRun REST endpoint — invoking the daily
    # pipeline with daily params. The two scheduled triggers (Daily, Monthly)
    # work without it. If the operator does not have Microsoft Entra permissions
    # to create app registrations, we degrade gracefully: log a warning, skip
    # Step 9, and flag WebhookStatus='disabled' in the result payload so the
    # customer portal knows to hide the "Run Now" button.
    # =========================================================================
    Write-Host "`n[9] Creating webhook Service Principal" -ForegroundColor Cyan

    $webhookSPName     = "$DataFactoryName-trigger-sp"
    $adfResourceId     = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName"
    $webhookTriggerUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/pipelines/$PipelineName/createRun?api-version=2018-06-01"
    $webhookTenantId   = (Get-AzContext).Tenant.Id
    $webhookStatus         = 'disabled'
    $webhookDisabledReason = $null

    # Probe: does the caller have directory permission to create app registrations?
    # Anyone can create app regs if the tenant's `allowedToCreateApps` is true.
    # Otherwise, the caller needs Application Developer / Application Administrator
    # / Cloud Application Administrator / Global Administrator.
    $canCreateApps = $false
    try {
        $authPolicyResp = Invoke-AzRestMethod -Method GET -Uri 'https://graph.microsoft.com/v1.0/policies/authorizationPolicy' -ErrorAction Stop
        if ($authPolicyResp.StatusCode -ge 200 -and $authPolicyResp.StatusCode -lt 300) {
            $authPolicy = $authPolicyResp.Content | ConvertFrom-Json
            if ($authPolicy.defaultUserRolePermissions.allowedToCreateApps) {
                $canCreateApps = $true
                Write-Host "  Tenant policy allows app-reg creation for all users." -ForegroundColor Gray
            } else {
                # Fall back to directory-role check
                $me = (Invoke-AzRestMethod -Method GET -Uri 'https://graph.microsoft.com/v1.0/me' -ErrorAction Stop).Content | ConvertFrom-Json
                $memberOfResp = Invoke-AzRestMethod -Method GET -Uri "https://graph.microsoft.com/v1.0/users/$($me.id)/memberOf" -ErrorAction Stop
                $roles = ($memberOfResp.Content | ConvertFrom-Json).value |
                    Where-Object { $_.'@odata.type' -eq '#microsoft.graph.directoryRole' } |
                    Select-Object -ExpandProperty displayName
                $eligibleRoles = @('Application Developer','Application Administrator','Cloud Application Administrator','Global Administrator')
                $matchedRoles  = $roles | Where-Object { $eligibleRoles -contains $_ }
                if ($matchedRoles) {
                    $canCreateApps = $true
                    Write-Host "  Caller holds eligible directory role: $($matchedRoles -join ', ')" -ForegroundColor Gray
                }
            }
        }
    } catch {
        Write-Host "  ⚠ Could not probe directory permissions: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    if (-not $canCreateApps) {
        $webhookDisabledReason = "Operator lacks Microsoft Entra ID permission to create app registrations in tenant '$webhookTenantId'. Tenant-wide 'Users can register applications' is disabled and the caller does not hold Application Developer / Application Administrator / Cloud Application Administrator / Global Administrator. Scheduled triggers (Daily, Monthly) will still run normally. On-demand runs from the customer portal will be unavailable until a directory-privileged operator re-runs this deploy, or an SP is provisioned manually."
        Write-Host "⚠ Skipping webhook SP creation — insufficient directory privileges." -ForegroundColor Yellow
        Write-Host "  Reason: $webhookDisabledReason" -ForegroundColor Yellow
        Write-Host "  Scheduled triggers still work; only on-demand runs are disabled." -ForegroundColor Yellow
        $webhookClientId     = $null
        $webhookClientSecret = $null
    } else {
        try {
            # Check for an existing app with this name
            $existingApp = Get-AzADApplication -DisplayName $webhookSPName -ErrorAction SilentlyContinue | Select-Object -First 1

            if (-not $existingApp) {
                Write-Host "Creating app registration: $webhookSPName..." -ForegroundColor Yellow
                $webhookApp = New-AzADApplication -DisplayName $webhookSPName -ErrorAction Stop
            } else {
                Write-Host "App registration already exists: $webhookSPName" -ForegroundColor Yellow
                $webhookApp = $existingApp
            }

            # Ensure a Service Principal exists for the app
            $webhookSP = Get-AzADServicePrincipal -ApplicationId $webhookApp.AppId -ErrorAction SilentlyContinue
            if (-not $webhookSP) {
                Write-Host "Creating service principal..." -ForegroundColor Yellow
                $webhookSP = New-AzADServicePrincipal -ApplicationId $webhookApp.AppId -ErrorAction Stop
                Start-Sleep -Seconds 10  # Allow SP to propagate
            }

            # Generate a new client secret (valid 2 years)
            Write-Host "Generating client secret (2-year expiry)..." -ForegroundColor Yellow
            $credResult          = New-AzADAppCredential -ApplicationId $webhookApp.AppId -EndDate (Get-Date).AddYears(2) -ErrorAction Stop
            $webhookClientSecret = $credResult.SecretText
            $webhookClientId     = $webhookApp.AppId

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

            $webhookStatus = 'ready'
            Write-Host "✓ Webhook SP configured" -ForegroundColor Green
            Write-Host "  SP Name:    $webhookSPName" -ForegroundColor Gray
            Write-Host "  Client ID:  $webhookClientId" -ForegroundColor Gray
            Write-Host "  Tenant ID:  $webhookTenantId" -ForegroundColor Gray
            Write-Host "  Trigger URI: $webhookTriggerUri" -ForegroundColor Gray
        } catch {
            # Probe said yes, but creation still failed — fall back gracefully
            $webhookDisabledReason = "Probe indicated sufficient privileges, but app-registration creation failed at runtime: $($_.Exception.Message). Scheduled triggers still work; on-demand runs disabled."
            Write-Host "⚠ Webhook SP creation failed mid-flight. $webhookDisabledReason" -ForegroundColor Yellow
            $webhookClientId     = $null
            $webhookClientSecret = $null
            $webhookStatus       = 'disabled'
        }
    }

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
    Write-Host "Destination:          $customerStorageAccount/$destContainerName/$normalizedPath" -ForegroundColor White
    Write-Host "Pipeline:             $PipelineName  (mirror whole month folder; target = today - $catchupCutoffDay d)" -ForegroundColor White
    Write-Host "TimeZone:             $timeZone" -ForegroundColor White
    Write-Host "Triggers:" -ForegroundColor White
    Write-Host "  $DailyTriggerName    — every day at $dailyRunTime" -ForegroundColor White
    Write-Host "  $MonthlyTriggerName  — day $monthlyDayOfMonth at $monthlyRunTime" -ForegroundColor White
    Write-Host "  (on-demand runs invoke the daily pipeline via webhook)" -ForegroundColor White
    if ($usePrivateEndpoint) {
        Write-Host "Integration Runtime:  $IRName (Managed VNet)" -ForegroundColor White
        Write-Host "Private Endpoint:     $ManagedPEName" -ForegroundColor White
    }
    Write-Host "Webhook Status:       $webhookStatus" -ForegroundColor $(if ($webhookStatus -eq 'ready') { 'White' } else { 'Yellow' })
    if ($webhookStatus -eq 'ready') {
        Write-Host "Webhook SP:           $webhookSPName" -ForegroundColor White
        Write-Host "Webhook Tenant ID:    $webhookTenantId" -ForegroundColor White
        Write-Host "Webhook Client ID:    $webhookClientId" -ForegroundColor White
        Write-Host "Webhook Trigger URI:  $webhookTriggerUri" -ForegroundColor White
    } else {
        Write-Host "Webhook Disabled:     $webhookDisabledReason" -ForegroundColor Yellow
        Write-Host "  On-demand runs from the customer portal will be unavailable." -ForegroundColor Yellow
        Write-Host "  Scheduled runs (Daily, Monthly) are unaffected." -ForegroundColor Yellow
    }
    Write-Host ("=" * 80) -ForegroundColor Green

    # Webhook usage instructions — on-demand fires the daily pipeline
    if ($webhookStatus -ne 'ready') {
        Write-Host "`nOn-demand webhook is NOT available for this site (see Webhook Disabled reason above)." -ForegroundColor Yellow
    }
    Write-Host "`nTo trigger an on-demand run via webhook (PowerShell):" -ForegroundColor Yellow
    Write-Host @"
`$tokenBody = `"grant_type=client_credentials&client_id=$webhookClientId&client_secret=<SECRET>&resource=https://management.azure.com/"
`$tokenResp = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$webhookTenantId/oauth2/token" -Method Post -Body `$tokenBody
`$body = @{
    sourceFolderName = '$dailiesFolder'
    catchupDays      = $catchupCutoffDay
    mode             = 'daily'
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
    -Parameter @{ sourceFolderName = '$dailiesFolder'; catchupDays = $catchupCutoffDay; mode = 'daily' }
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
    $results | Add-Member -MemberType NoteProperty -Name DestStorage         -Value $customerStorageAccount
    $results | Add-Member -MemberType NoteProperty -Name DestContainer       -Value $destContainerName
    $results | Add-Member -MemberType NoteProperty -Name DestFolder          -Value $normalizedPath
    $results | Add-Member -MemberType NoteProperty -Name Pipeline            -Value $PipelineName
    $results | Add-Member -MemberType NoteProperty -Name TimeZone            -Value $timeZone
    $results | Add-Member -MemberType NoteProperty -Name DailyTrigger        -Value $DailyTriggerName
    $results | Add-Member -MemberType NoteProperty -Name DailyRunTime        -Value $dailyRunTime
    $results | Add-Member -MemberType NoteProperty -Name MonthlyTrigger      -Value $MonthlyTriggerName
    $results | Add-Member -MemberType NoteProperty -Name MonthlyDayOfMonth   -Value $monthlyDayOfMonth
    $results | Add-Member -MemberType NoteProperty -Name MonthlyRunTime      -Value $monthlyRunTime
    $results | Add-Member -MemberType NoteProperty -Name CatchupCutoffDay    -Value $catchupCutoffDay
    $results | Add-Member -MemberType NoteProperty -Name IntegrationRuntime  -Value $(if ($usePrivateEndpoint) { $IRName } else { "AutoResolveIntegrationRuntime" })
    $results | Add-Member -MemberType NoteProperty -Name PrivateEndpoint     -Value $(if ($usePrivateEndpoint) { $ManagedPEName } else { $null })
    $results | Add-Member -MemberType NoteProperty -Name ScheduleStartTime   -Value $ScheduleStartTime
    $results | Add-Member -MemberType NoteProperty -Name WebhookStatus         -Value $webhookStatus
    $results | Add-Member -MemberType NoteProperty -Name WebhookDisabledReason -Value $webhookDisabledReason
    $results | Add-Member -MemberType NoteProperty -Name WebhookSPName         -Value $(if ($webhookStatus -eq 'ready') { $webhookSPName } else { $null })
    $results | Add-Member -MemberType NoteProperty -Name WebhookTenantId       -Value $(if ($webhookStatus -eq 'ready') { $webhookTenantId } else { $null })
    $results | Add-Member -MemberType NoteProperty -Name WebhookClientId       -Value $webhookClientId
    $results | Add-Member -MemberType NoteProperty -Name WebhookClientSecret   -Value $webhookClientSecret
    $results | Add-Member -MemberType NoteProperty -Name WebhookTriggerUri     -Value $(if ($webhookStatus -eq 'ready') { $webhookTriggerUri } else { $null })

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
