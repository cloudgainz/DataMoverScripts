param(
    [parameter(Mandatory)]
    [hashtable]$parameterTable
)
# tease out parameters that I care about
$location = $parameterTable.location
$siteName = $parameterTable.siteName
$ResourceGroupName = $parameterTable.runBookRG
$subscriptionName = $parameterTable.subscriptionName
$vnetSubnetId = $parameterTable.vnetSubnetId  # optional - triggers Hybrid Worker mode if present

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

# Hybrid Worker VM naming
[string]$VMName = $siteName + "-hw-vm"
[string]$NICName = $siteName + "-hw-nic"
[string]$WorkerGroupName = $siteName + "-HybridWorkerGroup"
[string]$VMSize = "Standard_B2s"
[string]$VMAdminUser = "hwadmin"

$useHybridWorker = -not [string]::IsNullOrWhiteSpace($vnetSubnetId)

if ($useHybridWorker) {
    Write-Host "Mode: Hybrid Worker (Linux) - Subnet: $vnetSubnetId" -ForegroundColor Magenta
} else {
    Write-Host "Mode: Cloud Automation Account" -ForegroundColor Magenta
}

# Set error action preference
$ErrorActionPreference = "Stop"

# Import required modules
Write-Host "Checking for required Azure modules..." -ForegroundColor Cyan
$requiredModules = @("Az.Accounts", "Az.Automation", "Az.Resources")
if ($useHybridWorker) {
    $requiredModules += @("Az.Compute", "Az.Network")
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

# Set default webhook name if not provided
if (-not $WebhookName) {
    $WebhookName = "$RunbookName-Webhook"
}

try {
    # Step 1: Create or verify Resource Group
    Write-Host "`n[1] Checking Resource Group: $ResourceGroupName" -ForegroundColor Cyan
    $rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
    if (-not $rg) {
        Write-Host "Creating Resource Group: $ResourceGroupName in $Location" -ForegroundColor Yellow
        $rg = New-AzResourceGroup -Name $ResourceGroupName -Location $Location -Tag $Tags
        Write-Host "✓ Resource Group created successfully" -ForegroundColor Green
    } else {
        Write-Host "✓ Resource Group already exists" -ForegroundColor Green
    }

    # Step 2: Create Automation Account
    Write-Host "`n[2] Creating Azure Automation Account: $AutomationAccountName" -ForegroundColor Cyan
    $automationAccount = Get-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -ErrorAction SilentlyContinue

    if (-not $automationAccount) {
        $automationAccount = New-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -Location $Location -Plan Basic -Tags $Tags

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
    Write-Host "`n[3] Configuring Managed Identity and Storage Permissions" -ForegroundColor Cyan

    # Enable system-assigned managed identity
    Write-Host "Enabling system-assigned managed identity..." -ForegroundColor Yellow
    $identity = Set-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName -AssignSystemIdentity

    $principalId = $identity.Identity.PrincipalId
    Write-Host "✓ Managed identity enabled" -ForegroundColor Green
    Write-Host "  Principal ID: $principalId" -ForegroundColor Gray

    $exportStorageAccount = $parameterTable.exportStorageAccount
    Write-Host "Granting required roles to $exportStorageAccount..." -ForegroundColor Yellow

    $storageAccount = Get-AzStorageAccount | Where-Object { $_.StorageAccountName -eq $exportStorageAccount }
    if (-not $storageAccount) {
        Write-Host "⚠ Warning: Export storage account '$exportStorageAccount' not found in current subscription" -ForegroundColor Yellow
        Write-Host "  You may need to manually grant permissions if the storage account is in a different subscription" -ForegroundColor Yellow
    } else {
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
                            throw "Failed to assign $roleName role after $tryTotal attempts: $_"
                        }
                        Write-Host "Failed to assign $roleName role, retrying in 5 seconds... (Attempt $tryCount of $tryTotal)" -ForegroundColor Yellow
                        Start-Sleep -Seconds 5
                    }
                }
            } else {
                Write-Host "✓ $roleName role already assigned" -ForegroundColor Green
            }
        }
    }

    # Step 4: Download and import the runbook
    Write-Host "`n[4] Importing Runbook: $RunbookName from $RunbookScriptUri" -ForegroundColor Cyan

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
    Start-Sleep -Seconds 5
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
    Write-Host "`n[5] Creating Automation Variable for parameterTable" -ForegroundColor Cyan

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

    # -------------------------------------------------------------------------
    # Step 6 (Hybrid Worker only): Create VM, NIC, Worker Group, and Extension
    # -------------------------------------------------------------------------
    $sshPrivateKeyPath = $null
    if ($useHybridWorker) {
        Write-Host "`n[6] Setting up Hybrid Worker (vnetSubnetId present)" -ForegroundColor Cyan

        # Generate SSH key pair
        Write-Host "Generating SSH key pair..." -ForegroundColor Yellow
        $sshKeyName = "$siteName-hw-key"
        $sshPrivateKeyPath = Join-Path $env:USERPROFILE ".ssh" "$sshKeyName"
        $sshDir = Split-Path $sshPrivateKeyPath

        if (-not (Test-Path $sshDir)) {
            New-Item -ItemType Directory -Path $sshDir -Force | Out-Null
        }

        # Generate key using ssh-keygen
        $tempKeyPath = Join-Path $env:TEMP $sshKeyName
        ssh-keygen -t rsa -b 4096 -f $tempKeyPath -N "" -C "hwadmin@$siteName" -q

        # Read keys
        $sshPublicKey = Get-Content "$tempKeyPath.pub"
        $sshPrivateKeyContent = Get-Content $tempKeyPath -Raw

        # Save private key with restricted permissions
        Set-Content -Path $sshPrivateKeyPath -Value $sshPrivateKeyContent -Force
        (Get-Item $sshPrivateKeyPath).Attributes = "Hidden"

        # Clean up temp files
        Remove-Item $tempKeyPath -Force
        Remove-Item "$tempKeyPath.pub" -Force

        Write-Host "✓ SSH key pair generated" -ForegroundColor Green
        Write-Host "  Private key saved to: $sshPrivateKeyPath" -ForegroundColor Gray

        # Create dummy credential for VM config (username only)
        $securePassword = ConvertTo-SecureString "dummy" -AsPlainText -Force
        $vmCredential = New-Object System.Management.Automation.PSCredential($VMAdminUser, $securePassword)

        # Create NIC in the specified subnet
        Write-Host "Creating Network Interface in subnet..." -ForegroundColor Yellow
        $existingNic = Get-AzNetworkInterface -Name $NICName -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        if (-not $existingNic) {
            $nic = New-AzNetworkInterface -Name $NICName -ResourceGroupName $ResourceGroupName -Location $Location -SubnetId $vnetSubnetId -Tag $Tags
            Write-Host "✓ NIC created: $NICName (Private IP: $($nic.IpConfigurations[0].PrivateIpAddress))" -ForegroundColor Green
        } else {
            $nic = $existingNic
            Write-Host "✓ NIC already exists: $NICName (Private IP: $($nic.IpConfigurations[0].PrivateIpAddress))" -ForegroundColor Green
        }

        # Create VM (Windows or Linux)
        $osDisplay = if ($HybridWorkerOS -eq "Windows") { "Windows Server 2025" } else { "Ubuntu 22.04 LTS" }
        Write-Host "Creating Hybrid Worker VM: $VMName ($VMSize, $osDisplay)..." -ForegroundColor Yellow
        $existingVM = Get-AzVM -Name $VMName -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
        if (-not $existingVM) {
            # Create Ubuntu 22.04 VM
            $vmConfig = New-AzVMConfig -VMName $VMName -VMSize $VMSize -Tags $Tags |
                Set-AzVMOperatingSystem -Linux -ComputerName ($siteName -replace '_','-') -Credential $vmCredential -DisablePasswordAuthentication |
                Add-AzVMSshPublicKey -KeyData $sshPublicKey -Path "/home/$VMAdminUser/.ssh/authorized_keys" |
                Set-AzVMSourceImage -PublisherName "Canonical" -Offer "0001-com-ubuntu-server-jammy" -Skus "22_04-lts-gen2" -Version "latest" |
                Add-AzVMNetworkInterface -Id $nic.Id |
                Set-AzVMBootDiagnostic -Disable

            New-AzVM -ResourceGroupName $ResourceGroupName -Location $Location -VM $vmConfig -Tag $Tags | Out-Null

            Write-Host "Waiting for VM to be fully provisioned..." -ForegroundColor Yellow
            Start-Sleep -Seconds 30
            $vmCheck = Get-AzVM -Name $VMName -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
            while ($vmCheck.ProvisioningState -ne "Succeeded") {
                Write-Host "  VM provisioning state: $($vmCheck.ProvisioningState)..." -ForegroundColor Yellow
                Start-Sleep -Seconds 10
                $vmCheck = Get-AzVM -Name $VMName -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue
            }
            Write-Host "✓ VM created successfully" -ForegroundColor Green
        } else {
            Write-Host "✓ VM already exists: $VMName" -ForegroundColor Green
        }

        # Enable system-assigned managed identity on the VM
        Write-Host "Enabling managed identity on VM..." -ForegroundColor Yellow
        $vm = Get-AzVM -Name $VMName -ResourceGroupName $ResourceGroupName
        if ($vm.Identity.Type -ne "SystemAssigned" -and $vm.Identity.Type -ne "SystemAssigned, UserAssigned") {
            Update-AzVM -ResourceGroupName $ResourceGroupName -VM $vm -IdentityType SystemAssigned | Out-Null
            Start-Sleep -Seconds 10
            $vm = Get-AzVM -Name $VMName -ResourceGroupName $ResourceGroupName
        }
        Write-Host "✓ VM managed identity enabled (Principal ID: $($vm.Identity.PrincipalId))" -ForegroundColor Green

        # Install PowerShell 7 (required for PowerShell72 runbooks)
        Write-Host "Installing PowerShell 7..." -ForegroundColor Yellow
        $installPwshScript = @'
#!/bin/bash
set -e
if command -v pwsh &> /dev/null; then
    echo "PowerShell 7 already installed at: $(which pwsh)"
else
    echo "Installing PowerShell 7 via snap..."
    snap install powershell --classic
    echo "PowerShell 7 installed successfully"
fi

# Find pwsh and set environment variable
pwshPath=$(which pwsh)
if [ -n "$pwshPath" ]; then
    echo "powershell_7_2_path=$pwshPath" >> /etc/environment
    echo "Environment variable set to: $pwshPath"
else
    echo "ERROR: pwsh not found in PATH after installation"
    exit 1
fi
'@
        $commandId = "RunShellScript"

        $installResult = Invoke-AzVMRunCommand -ResourceGroupName $ResourceGroupName -VMName $VMName -CommandId $commandId -ScriptString $installPwshScript
        Write-Host "  $($installResult.Value[0].Message)" -ForegroundColor Gray
        Write-Host "✓ PowerShell 7 installation complete" -ForegroundColor Green

        # Install Azure PowerShell modules required for runbook execution
        Write-Host "Installing Azure PowerShell modules on VM..." -ForegroundColor Yellow
        $bashScript = @'
#!/bin/bash
/snap/bin/pwsh << 'PWSHEOF'
$requiredModules = @('Az.Accounts', 'Az.Storage', 'Az.Automation')
$progressPreference = 'SilentlyContinue'
foreach ($module in $requiredModules) {
    Write-Output "Installing $module..."
    Install-Module -Name $module -Repository PSGallery -Force -AllowClobber -Scope AllUsers -ErrorAction SilentlyContinue
    Write-Output "  [OK] $module installed"
}
Write-Output "[OK] All required modules installed successfully"
PWSHEOF
'@

        $moduleInstallResult = Invoke-AzVMRunCommand -ResourceGroupName $ResourceGroupName -VMName $VMName -CommandId "RunShellScript" -ScriptString $bashScript
        Write-Host "  $($moduleInstallResult.Value[0].Message)" -ForegroundColor Gray
        Write-Host "✓ Azure PowerShell modules installation complete" -ForegroundColor Green

        # Create Hybrid Worker Group
        Write-Host "Creating Hybrid Worker Group: $WorkerGroupName..." -ForegroundColor Yellow
        $existingGroup = Get-AzAutomationHybridRunbookWorkerGroup -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $WorkerGroupName -ErrorAction SilentlyContinue
        if (-not $existingGroup) {
            New-AzAutomationHybridRunbookWorkerGroup -AutomationAccountName $AutomationAccountName -Name $WorkerGroupName -ResourceGroupName $ResourceGroupName
            Write-Host "✓ Hybrid Worker Group created" -ForegroundColor Green
        } else {
            Write-Host "✓ Hybrid Worker Group already exists" -ForegroundColor Green
        }

        # Register VM as Hybrid Worker
        $vmResourceId = $vm.Id
        $existingWorkers = Get-AzAutomationHybridRunbookWorker -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -HybridRunbookWorkerGroupName $WorkerGroupName -ErrorAction SilentlyContinue
        $alreadyRegistered = $existingWorkers | Where-Object { $_.VmResourceId -eq $vmResourceId }
        if (-not $alreadyRegistered) {
            Write-Host "Registering VM as Hybrid Worker..." -ForegroundColor Yellow
            New-AzAutomationHybridRunbookWorker -Name ([guid]::NewGuid().ToString()) -VmResourceId $vmResourceId -HybridRunbookWorkerGroupName $WorkerGroupName -AutomationAccountName $AutomationAccountName -ResourceGroupName $ResourceGroupName
            Write-Host "✓ VM registered as Hybrid Worker" -ForegroundColor Green
        } else {
            Write-Host "✓ VM already registered as Hybrid Worker" -ForegroundColor Green
        }

        # Install Hybrid Worker Extension
        Write-Host "Installing Hybrid Worker Extension on VM..." -ForegroundColor Yellow
        $aaResource = Get-AzResource -ResourceGroupName $ResourceGroupName -ResourceType "Microsoft.Automation/automationAccounts" -Name $AutomationAccountName
        $aaDetails = Get-AzResource -ResourceId $aaResource.ResourceId -ApiVersion "2023-11-01"
        $extensionSettings = @{ "AutomationAccountURL" = $aaDetails.Properties.automationHybridServiceUrl }

        # Install Hybrid Worker Extension
        $extensionType = "HybridWorkerForLinux"
        $existingExtension = Get-AzVMExtension -ResourceGroupName $ResourceGroupName -VMName $VMName -Name "HybridWorkerExtension" -ErrorAction SilentlyContinue
        if (-not $existingExtension) {
            Set-AzVMExtension -ResourceGroupName $ResourceGroupName -Location $Location -VMName $VMName -Name "HybridWorkerExtension" -Publisher "Microsoft.Azure.Automation.HybridWorker" -ExtensionType $extensionType -TypeHandlerVersion "1.1" -Settings $extensionSettings -EnableAutomaticUpgrade $true
            Write-Host "✓ Hybrid Worker Extension installed ($extensionType)" -ForegroundColor Green
        } else {
            Write-Host "✓ Hybrid Worker Extension already installed" -ForegroundColor Green
        }

        # Restart Hybrid Worker service
        Write-Host "Restarting Hybrid Worker service..." -ForegroundColor Yellow
        $restartResult = Invoke-AzVMRunCommand -ResourceGroupName $ResourceGroupName -VMName $VMName -CommandId "RunShellScript" -ScriptString 'sudo systemctl restart waagent; echo "Hybrid Worker service restarted"'
        Write-Host "  $($restartResult.Value[0].Message)" -ForegroundColor Gray
        Write-Host "✓ Hybrid Worker service restarted" -ForegroundColor Green
    }

    # Step 7 (was 6): Create Schedule
    Write-Host "`n[7] Creating Schedule: $ScheduleName" -ForegroundColor Cyan

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

    # Step 8 (was 7): Link Schedule to Runbook
    Write-Host "`n[8] Linking Schedule to Runbook" -ForegroundColor Cyan

    $scheduleParams = @{
        ResourceGroupName     = $ResourceGroupName
        AutomationAccountName = $AutomationAccountName
        RunbookName           = $RunbookName
        ScheduleName          = $ScheduleName
    }
    if ($useHybridWorker) {
        $scheduleParams["RunOn"] = $WorkerGroupName
        Write-Host "  Targeting Hybrid Worker Group: $WorkerGroupName" -ForegroundColor Gray
    }
    Register-AzAutomationScheduledRunbook @scheduleParams

    Write-Host "✓ Schedule linked to runbook successfully" -ForegroundColor Green

    # Step 9 (was 8): Create Webhook
    Write-Host "`n[9] Creating Webhook: $WebhookName" -ForegroundColor Cyan

    # Check if webhook exists and remove it
    $existingWebhook = Get-AzAutomationWebhook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $WebhookName -ErrorAction SilentlyContinue

    if ($existingWebhook) {
        Write-Host "Webhook already exists. Removing old webhook..." -ForegroundColor Yellow
        Remove-AzAutomationWebhook -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name $WebhookName
    }

    # Create webhook with expiry
    $webhookExpiryDate = (Get-Date).AddYears($WebhookExpiryYears)

    $webhookParams = @{
        ResourceGroupName     = $ResourceGroupName
        AutomationAccountName = $AutomationAccountName
        RunbookName           = $RunbookName
        Name                  = $WebhookName
        IsEnabled             = $true
        ExpiryTime            = $webhookExpiryDate
        Force                 = $true
    }
    if ($useHybridWorker) {
        $webhookParams["RunOn"] = $WorkerGroupName
    }
    $webhook = New-AzAutomationWebhook @webhookParams

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
    Write-Host "Mode:                 $(if ($useHybridWorker) { 'Hybrid Worker' } else { 'Cloud Automation' })" -ForegroundColor White
    Write-Host "Resource Group:       $ResourceGroupName" -ForegroundColor White
    Write-Host "Automation Account:   $AutomationAccountName" -ForegroundColor White
    Write-Host "Location:             $Location" -ForegroundColor White
    if ($useHybridWorker) {
        Write-Host "Hybrid Worker VM:     $VMName ($VMSize)" -ForegroundColor White
        Write-Host "VM Operating System:  Ubuntu 22.04 LTS" -ForegroundColor White
        Write-Host "Hybrid Worker Group:  $WorkerGroupName" -ForegroundColor White
        Write-Host "VM Subnet:            $vnetSubnetId" -ForegroundColor White
        Write-Host "SSH Private Key:      $sshPrivateKeyPath" -ForegroundColor Cyan
    }
    Write-Host "Runbook:              $RunbookName (Published)" -ForegroundColor White
    Write-Host "Variable:             $variableName (parameterTable stored)" -ForegroundColor White
    Write-Host "Schedule:             $ScheduleName (Every $ScheduleInterval $ScheduleFrequency(s))" -ForegroundColor White
    Write-Host "Webhook:              $WebhookName (Expires: $webhookExpiryDate)" -ForegroundColor White
    Write-Host ("=" * 80) -ForegroundColor Green

    # Return object with all details
    $results = [PSCustomObject] @{
        Mode              = if ($useHybridWorker) { "HybridWorker" } else { "CloudAutomation" }
        ResourceGroup     = $ResourceGroupName
        AutomationAccount = $AutomationAccountName
        Location          = $Location
        HybridWorkerVM    = if ($useHybridWorker) { $VMName } else { $null }
        HybridWorkerGroup = if ($useHybridWorker) { $WorkerGroupName } else { $null }
        VMOperatingSystem = if ($useHybridWorker) { "Ubuntu 22.04 LTS" } else { $null }
        VMAdminUser       = if ($useHybridWorker) { $VMAdminUser } else { $null }
        SSHPrivateKeyPath = if ($useHybridWorker) { $sshPrivateKeyPath } else { $null }
        Runbook           = $RunbookName
        Variable          = $variableName
        Schedule          = $ScheduleName
        ScheduleFrequency = "$ScheduleInterval $ScheduleFrequency(s)"
        ScheduleStartTime = $ScheduleStartTime
        Webhook           = $WebhookName
        WebhookURI        = $webhook.WebhookURI
        WebhookExpiry     = $webhookExpiryDate
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
