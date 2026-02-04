# Get the variable value (JSON string)
$variable = Get-AzAutomationVariable -ResourceGroupName 'CI-IMAGES' -AutomationAccountName 'TestSite01-aa' -Name 'TestSite01-ParameterTable'

# Convert back to hashtable to view
$retrievedTable = $variable.Value | ConvertFrom-Json -AsHashtable

# View the hashtable
$retrievedTable