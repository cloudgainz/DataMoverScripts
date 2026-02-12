param(
    [parameter(Mandatory)]
    [hashtable]$parameterTable
)
# tease out parameters that I care about
$location = $parameterTable.location
$siteName = $parameterTable.siteName
$ResourceGroupName = $parameterTable.runBookRG
$subscriptionName = $parameterTable.subscriptionName 

Set-AzContext -Subscription $subscriptionName

[string]$ResourceGroupName =  $parameterTable.runBookRG
[string]$AutomationAccountName = $siteName + "-aa"
[string]$RunbookName = $siteName + "-DataMoverRunbook"
[string]$RunbookScriptUri = "https://raw.githubusercontent.com/cloudgainz/DataMoverScripts/refs/heads/main/PowerShell/Workbook.ps1"
[string]$ScheduleName = $siteName + "-DataMoverSchedule"
# [ValidateSet("Hour", "Day", "Week", "Month")]
[string]$ScheduleFrequency = "Day"
[int]$ScheduleInterval = 1
[DateTime]$ScheduleStartTime = (Get-Date).AddDays(1).Date.AddHours(2)
[string]$WebhookName = $siteName + "-Webhook"
[int]$WebhookExpiryYears = 1
[hashtable]$Tags = @{}

# Set error action preference
$ErrorActionPreference = "Stop"

# Import required modules
Write-Host "Checking for required Azure modules..." -ForegroundColor Cyan
$requiredModules = @("Az.Accounts", "Az.Automation", "Az.Resources")

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

# Set default webhook name if not provided
if (-not $WebhookName) {
    $WebhookName = "$RunbookName-Webhook"
}

try {
    # Step 1: Create or verify Resource Group
    Write-Host "`n[1/6] Checking Resource Group: $ResourceGroupName" -ForegroundColor Cyan
    $rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
    if (-not $rg) {
        Write-Host "Creating Resource Group: $ResourceGroupName in $Location" -ForegroundColor Yellow
        $rg = New-AzResourceGroup -Name $ResourceGroupName -Location $Location -Tag $Tags
        Write-Host "✓ Resource Group created successfully" -ForegroundColor Green
    } else {
        Write-Host "✓ Resource Group already exists" -ForegroundColor Green
    }

    # Step 2: Create Automation Account
    Write-Host "`n[2/6] Creating Azure Automation Account: $AutomationAccountName" -ForegroundColor Cyan
    $automationAccount = Get-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -ErrorAction SilentlyContinue
    
    if (-not $automationAccount) {
        $automationAccount = New-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -Location $Location -Plan Basic -Tags $Tags # -- Trap this in case of failure and provide a helpful error message -- wait for account

        Start-Sleep -Seconds 10 # Wait for the account to be fully provisioned before proceeding

        $aatest = Get-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -ErrorAction SilentlyContinue
        while ($aatest -eq $null) {
            Write-Host "Waiting for Automation Account to be provisioned..." -ForegroundColor Yellow
            Start-Sleep -Seconds 5
            $aatest = Get-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -ErrorAction SilentlyContinue
        }
        
        Write-Host "✓ Automation Account created successfully" -ForegroundColor Green
        Write-Host "  Account ID: $($automationAccount.Identity.PrincipalId)" -ForegroundColor Gray
    } else {
        Write-Host "✓ Automation Account already exists" -ForegroundColor Green
    }

    # Step 3: Configure Managed Identity and Storage Permissions
    Write-Host "`n[3/8] Configuring Managed Identity and Storage Permissions" -ForegroundColor Cyan
    
    # Enable system-assigned managed identity
    Write-Host "Enabling system-assigned managed identity..." -ForegroundColor Yellow
    $identity = Set-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -AssignSystemIdentity
    
    $principalId = $identity.Identity.PrincipalId
    Write-Host "✓ Managed identity enabled" -ForegroundColor Green
    Write-Host "  Principal ID: $principalId" -ForegroundColor Gray
    
    # Get the export storage account
    $exportStorageAccount = $parameterTable.exportStorageAccount
    Write-Host "Granting required roles to $exportStorageAccount..." -ForegroundColor Yellow

    # Try to retrieve storage account by name. Some Az module versions / parameter sets
    # can raise a parameter-set error in Automation runbooks; fall back to enumerating accounts.
    $storageAccount = $null
    try {
        $storageAccount = Get-AzStorageAccount -Name $exportStorageAccount -ErrorAction Stop
    }
    catch {
        Write-Host "Get-AzStorageAccount -Name failed: $($_.Exception.Message). Falling back to enumerate all storage accounts..." -ForegroundColor Yellow
        $storageAccount = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount } | Select-Object -First 1
    }

    if (-not $storageAccount) {
        Write-Host "⚠ Warning: Export storage account '$exportStorageAccount' not found in current subscription" -ForegroundColor Yellow
        Write-Host "  You may need to manually grant permissions if the storage account is in a different subscription" -ForegroundColor Yellow
    } else {
        # Assign management-plane Reader role (needed for Get-AzStorageAccount)
        $readerAssignment = Get-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName "Reader" -Scope $storageAccount.Id -ErrorAction SilentlyContinue
        if (-not $readerAssignment) {
            $tryTotal = 5
            $tryCount = 0
            while ($tryCount -lt $tryTotal) {
                try {
                    New-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName "Reader" -Scope $storageAccount.Id | Out-Null
                    Write-Host "✓ Reader role assigned on storage account" -ForegroundColor Green
                    break
                } catch {
                    $tryCount++
                    if ($tryCount -ge $tryTotal) {
                        throw "Failed to assign Reader role after $tryTotal attempts: $_"
                    }
                    Write-Host "Failed to assign Reader role, retrying in 5 seconds... (Attempt $tryCount of $tryTotal)" -ForegroundColor Yellow
                    Start-Sleep -Seconds 5
                }
            }
        } else {
            Write-Host "✓ Reader role already assigned on storage account" -ForegroundColor Green
        }

        # Assign Storage Blob Data Reader role (data-plane)
        $roleAssignment = Get-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName "Storage Blob Data Reader" -Scope $storageAccount.Id -ErrorAction SilentlyContinue
        if (-not $roleAssignment) {
            $tryTotal = 5
            $tryCount = 0 
            while ($tryCount -lt $tryTotal) {
                try {
                    New-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName "Storage Blob Data Reader" -Scope $storageAccount.Id | Out-Null
                    Write-Host "✓ Storage Blob Data Reader role assigned" -ForegroundColor Green
                    break
                } catch {
                    $tryCount++
                    if ($tryCount -ge $tryTotal) {
                        throw "Failed to assign Storage Blob Data Reader role after $tryTotal attempts: $_"
                    }
                    Write-Host "Failed to assign Storage Blob Data Reader role, retrying in 5 seconds... (Attempt $tryCount of $tryTotal)" -ForegroundColor Yellow
                    Start-Sleep -Seconds 5
                }
            }
        } else {
            Write-Host "✓ Storage Blob Data Reader role already assigned" -ForegroundColor Green
        }
    }

    # Step 4: Download and import the runbook
    Write-Host "`n[4/8] Importing Runbook: $RunbookName from $RunbookScriptUri" -ForegroundColor Cyan
    
    # Download the script to a temporary location
    Write-Host "Downloading script from URI..." -ForegroundColor Yellow

    $tempPath = $RunbookScriptUri.Split('/')[-1]

    try {
        Remove-Item $tempPath -Force -ErrorAction SilentlyContinue
        Invoke-RestMethod -Uri $RunbookScriptUri -OutFile $tempPath -UseBasicParsing 
        Write-Host "✓ Script downloaded successfully" -ForegroundColor Green
    } catch {
        throw "Failed to download script from URI: $_"
    }

    # Modify the script to inject the automation variable retrieval
    Write-Host "Injecting automation variable reference..." -ForegroundColor Yellow
    $scriptContent = Get-Content -Path $tempPath -Raw
    # $variableName = "$siteName-ParameterTable"
    # $variableRetrieval = "[hashtable]`$parameterTable = Get-AutomationVariable -Name '$variableName' | ConvertFrom-Json -AsHashtable"
    
    # Replace the entire line with SITENAME-ParameterTable placeholder
    # $scriptContent = $scriptContent -replace '(?m)^\[hashtable\]\s*\$parameterTable\s*=\s*Get-AutomationVariable\s+-Name\s+.SITENAME-ParameterTable.\s*\|.*$', $variableRetrieval
    $siteNameTable = "$siteName-ParameterTable"
    $scriptContent = $scriptContent.replace("XXSITETABLEXX",$siteNameTable)
    Set-Content -Path $tempPath -Value $scriptContent -Force
    Write-Host "✓ Automation variable reference injected" -ForegroundColor Green

    # Import the runbook
    Write-Host "Importing runbook into Automation Account..." -ForegroundColor Yellow
    Import-AzAutomationRunbook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $RunbookName -Path $tempPath -Type PowerShell72 -Force

    Write-Host "✓ Runbook imported successfully" -ForegroundColor Green

    # Publish the runbook
    Write-Host "Publishing runbook..." -ForegroundColor Yellow
    Publish-AzAutomationRunbook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $RunbookName
    
    Write-Host "✓ Runbook published successfully" -ForegroundColor Green

    # Clean up temp file
    Remove-Item -Path $tempPath -Force -ErrorAction SilentlyContinue

    # Step 5: Create Automation Variable for parameterTable
    Write-Host "`n[5/8] Creating Automation Variable for parameterTable" -ForegroundColor Cyan
    
    $variableName = "$siteName-ParameterTable"
    
    # Check if variable exists and remove it
    $existingVariable = Get-AzAutomationVariable -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $variableName -ErrorAction SilentlyContinue

    if ($existingVariable) {
        Write-Host "Variable already exists. Removing old variable..." -ForegroundColor Yellow
        Remove-AzAutomationVariable -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $variableName
    }

    # Convert hashtable to JSON for storage
    $parameterTableJson = $parameterTable | ConvertTo-Json -Depth 10 -Compress
    
    New-AzAutomationVariable -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $variableName -Value $parameterTableJson -Encrypted $false

    Write-Host "✓ Automation Variable created successfully" -ForegroundColor Green
    Write-Host "  Variable Name: $variableName" -ForegroundColor Gray

    # Step 6: Create Schedule
    Write-Host "`n[6/8] Creating Schedule: $ScheduleName" -ForegroundColor Cyan
    
    # Check if schedule exists
    $existingSchedule = Get-AzAutomationSchedule -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $ScheduleName -ErrorAction SilentlyContinue

    if ($existingSchedule) {
        Write-Host "Schedule already exists. Removing old schedule..." -ForegroundColor Yellow
        Remove-AzAutomationSchedule -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $ScheduleName -Force
    }

   New-AzAutomationSchedule -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $ScheduleName -StartTime $ScheduleStartTime -TimeZone (Get-TimeZone).Id -DayInterval $ScheduleInterval

    Write-Host "✓ Schedule created successfully" -ForegroundColor Green
    Write-Host "  Frequency: Every $ScheduleInterval $ScheduleFrequency(s)" -ForegroundColor Gray
    Write-Host "  Start Time: $ScheduleStartTime" -ForegroundColor Gray
    Write-Host "  Time Zone: $((Get-TimeZone).Id)" -ForegroundColor Gray

    # Step 7: Link Schedule to Runbook
    Write-Host "`n[7/8] Linking Schedule to Runbook" -ForegroundColor Cyan
    
    Register-AzAutomationScheduledRunbook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -RunbookName $RunbookName -ScheduleName $ScheduleName
    
    Write-Host "✓ Schedule linked to runbook successfully" -ForegroundColor Green
    Write-Host "  Note: Runbook will retrieve parameterTable from automation variable: $variableName" -ForegroundColor Gray

    # Step 8: Create Webhook with authentication
    Write-Host "`n[8/8] Creating Webhook: $WebhookName" -ForegroundColor Cyan
    
    # Check if webhook exists and remove it
    $existingWebhook = Get-AzAutomationWebhook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $WebhookName -ErrorAction SilentlyContinue

    if ($existingWebhook) {
        Write-Host "Webhook already exists. Removing old webhook..." -ForegroundColor Yellow
        Remove-AzAutomationWebhook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $WebhookName 
    }

    # Create webhook with expiry
    $webhookExpiryDate = (Get-Date).AddYears($WebhookExpiryYears)
    
    $webhook = New-AzAutomationWebhook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -RunbookName $RunbookName -Name $WebhookName -IsEnabled $true -ExpiryTime $webhookExpiryDate -Force

    Write-Host "✓ Webhook created successfully" -ForegroundColor Green
    Write-Host "  Expires: $webhookExpiryDate" -ForegroundColor Gray

    # Display webhook URL (only shown once!)
    Write-Host "`n" + ("=" * 80) -ForegroundColor Yellow
    Write-Host "IMPORTANT: Save this Webhook URI - it will not be shown again!" -ForegroundColor Red
    Write-Host ("=" * 80) -ForegroundColor Yellow
    Write-Host $webhook.WebhookURI -ForegroundColor Cyan
    Write-Host ("=" * 80) -ForegroundColor Yellow
    Write-Host "`nTo trigger the runbook via webhook, use:" -ForegroundColor Yellow
    Write-Host @"
`$uri = "$($webhook.WebhookURI)"
`$headers = @{ "Content-Type" = "application/json" }
`$body = @{
    Parameter1 = "value1"
    Parameter2 = "value2"
} | ConvertTo-Json

Invoke-RestMethod -Uri `$uri -Method Post -Headers `$headers -Body `$body
"@ -ForegroundColor Gray

    # Summary
    Write-Host "`n" + ("=" * 80) -ForegroundColor Green
    Write-Host "SETUP COMPLETE!" -ForegroundColor Green
    Write-Host ("=" * 80) -ForegroundColor Green
    Write-Host "Resource Group:       $ResourceGroupName" -ForegroundColor White
    Write-Host "Automation Account:   $AutomationAccountName" -ForegroundColor White
    Write-Host "Location:             $Location" -ForegroundColor White
    Write-Host "Runbook:              $RunbookName (Published)" -ForegroundColor White
    Write-Host "Variable:             $variableName (parameterTable stored)" -ForegroundColor White
    Write-Host "Schedule:             $ScheduleName (Every $ScheduleInterval $ScheduleFrequency(s))" -ForegroundColor White
    Write-Host "Webhook:              $WebhookName (Expires: $webhookExpiryDate)" -ForegroundColor White
    Write-Host ("=" * 80) -ForegroundColor Green

    # Return object with all details

    $results = [PSCustomObject] @{
        ResourceGroup = $ResourceGroupName
        AutomationAccount = $AutomationAccountName
        Location = $Location
        Runbook = $RunbookName
        Variable = $variableName
        Schedule = $ScheduleName
        ScheduleFrequency = "$ScheduleInterval $ScheduleFrequency(s)"
        ScheduleStartTime = $ScheduleStartTime
        Webhook = $WebhookName
        WebhookURI = $webhook.WebhookURI
        WebhookExpiry = $webhookExpiryDate
    }

    $resultsJson = $results | ConvertTo-Json -Depth 5
    Write-Host "`nReturn Object (JSON):" -ForegroundColor Yellow

    # Upload results to customer storage account
    Write-Host "`nUploading results to customer storage..." -ForegroundColor Cyan
    try {
        $customerStorageAccount = $parameterTable.customerStorageAccount
        $customerToken = $parameterTable.customerToken
        $resultsFileName = "$siteName-runbook.json"
        $containerName = 'runbooks'
        
        # Create destination context using SAS token
        $destinationContext = New-AzStorageContext -StorageAccountName $customerStorageAccount -SasToken $customerToken
        
        # Create file in current directory
        $tempJsonPath = Join-Path (Get-Location) $resultsFileName
        $resultsJson | Out-File -FilePath $tempJsonPath -Encoding utf8 -Force
        
        # Create container if it doesn't exist
        $destinationContainer = Get-AzStorageContainer -Name $containerName -Context $destinationContext -ErrorAction SilentlyContinue
        if (-not $destinationContainer) {
            Write-Host "Creating destination container: $containerName" -ForegroundColor Yellow
            New-AzStorageContainer -Name $containerName -Context $destinationContext -Permission Off | Out-Null
        }
        
        # Upload to customer storage account
        Set-AzStorageBlobContent -File $tempJsonPath -Container $containerName -Blob $resultsFileName -Context $destinationContext -Force | Out-Null
        
        Write-Host "✓ Results uploaded: $resultsFileName to $customerStorageAccount/$containerName" -ForegroundColor Green
        
        # Clean up temp file
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
