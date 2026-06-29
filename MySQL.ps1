#
# MySQL.ps1 - PowerShell system for MySQL (MySqlConnector 2.4.0)
#

$Log_MaskableKeys = @(
    'password'
)

# Load bundled MySqlConnector assembly and dependencies
$script:LibPath = Join-Path (Split-Path $PSScriptRoot -Parent) 'lib' | Join-Path -ChildPath 'MySql'
$script:Dlls = @(
    'System.Runtime.CompilerServices.Unsafe.dll'
    'System.Threading.Tasks.Extensions.dll'
    'System.Buffers.dll'
    'System.Numerics.Vectors.dll'
    'System.Memory.dll'
    'Microsoft.Bcl.AsyncInterfaces.dll'
    'System.Diagnostics.DiagnosticSource.dll'
    'Microsoft.Extensions.DependencyInjection.Abstractions.dll'
    'Microsoft.Extensions.Logging.Abstractions.dll'
    'MySqlConnector.dll'
)

$script:MissingDlls = $script:Dlls | Where-Object { -not (Test-Path (Join-Path $script:LibPath $_)) }
if ($script:MissingDlls) {
    $Global:ModuleStatus = "<b><div class=`"alert alert-danger`" role=`"alert`">MySqlConnector library not installed. Run powershell command below as admin to download and install dependencies automatically.<br /><br /><code>. `"$($PSScriptRoot)\MySQL.ps1`"; Install-MySqlConnector</code><br /></div></b>"
} else {
    if (-not ([System.AppDomain]::CurrentDomain.GetAssemblies() | Where-Object { $_.GetName().Name -eq 'MySqlConnector' })) {
        $resolveHandler = [System.ResolveEventHandler] {
            param($sender, $e)
            $simpleName = ($e.Name -split ',')[0]
            $dllPath = Join-Path $script:LibPath "$simpleName.dll"
            if (Test-Path $dllPath) {
                return [System.Reflection.Assembly]::LoadFrom($dllPath)
            }
            return $null
        }
        [System.AppDomain]::CurrentDomain.add_AssemblyResolve($resolveHandler)

        foreach ($dll in $script:Dlls) {
            [System.Reflection.Assembly]::LoadFrom((Join-Path $script:LibPath $dll)) | Out-Null
        }
    }
    $Global:ModuleStatus = "<b><div class=`"alert alert-success`" role=`"alert`">Using MySqlConnector 2.4.0</div></b>"
}

function Install-MySqlConnector {
    <#
    .SYNOPSIS
        Downloads and installs MySqlConnector 2.4.0 dependencies.
    .DESCRIPTION
        Downloads NuGet packages from nuget.org, extracts netstandard2.0 DLLs,
        and places them in the lib\MySql folder.
    .PARAMETER Force
        Overwrite existing DLLs.
    #>
    param([switch]$Force)

    $Packages = @(
        @{ Name = 'System.Runtime.CompilerServices.Unsafe'; Version = '4.5.3' }
        @{ Name = 'System.Threading.Tasks.Extensions'; Version = '4.5.4' }
        @{ Name = 'System.Buffers'; Version = '4.4.0' }
        @{ Name = 'System.Numerics.Vectors'; Version = '4.5.0' }
        @{ Name = 'System.Memory'; Version = '4.5.5' }
        @{ Name = 'Microsoft.Bcl.AsyncInterfaces'; Version = '8.0.0' }
        @{ Name = 'System.Diagnostics.DiagnosticSource'; Version = '8.0.1' }
        @{ Name = 'Microsoft.Extensions.DependencyInjection.Abstractions'; Version = '8.0.2' }
        @{ Name = 'Microsoft.Extensions.Logging.Abstractions'; Version = '8.0.2' }
        @{ Name = 'MySqlConnector'; Version = '2.4.0' }
    )

    $target = $script:LibPath
    $null = New-Item -ItemType Directory -Path $target -Force
    $count = 0
    $total = $Packages.Count

    foreach ($pkg in $Packages) {
        $count++
        $dllName = "$($pkg.Name).dll"
        $outPath = Join-Path $target $dllName

        if ((Test-Path $outPath) -and -not $Force) {
            Write-Host "[$count/$total] $($pkg.Name) $($pkg.Version) - exists, skip (use -Force to overwrite)"
            continue
        }

        Write-Host "[$count/$total] $($pkg.Name) $($pkg.Version) ... " -NoNewline
        $nupkgUrl = "https://www.nuget.org/api/v2/package/$($pkg.Name)/$($pkg.Version)"
        $tmpZip = Join-Path $env:TEMP "$($pkg.Name).$($pkg.Version).zip"

        try { Invoke-WebRequest -Uri $nupkgUrl -OutFile $tmpZip -UseBasicParsing -ErrorAction Stop }
        catch { Write-Host "FAILED (download)"; continue }

        $extractDir = Join-Path $env:TEMP "$($pkg.Name).$($pkg.Version).ext"
        $null = New-Item -ItemType Directory -Path $extractDir -Force

        try { Expand-Archive -Path $tmpZip -DestinationPath $extractDir -Force -ErrorAction Stop }
        catch { Write-Host "FAILED (extract): $($_.Exception.Message)"; Remove-Item $tmpZip,$extractDir -Recurse -Force -ErrorAction SilentlyContinue; continue }

        $dllSource = Get-ChildItem -Path $extractDir -Recurse -Filter $dllName | Where-Object { $_.DirectoryName -match '\\netstandard2\.0$' } | Select-Object -First 1
        if (-not $dllSource) {
            $dllSource = Get-ChildItem -Path $extractDir -Recurse -Filter $dllName | Where-Object { $_.DirectoryName -match '\\lib\\' } | Sort-Object FullName | Select-Object -First 1
        }

        if (-not $dllSource) { Write-Host "FAILED (DLL not found)" }
        else { Copy-Item -Path $dllSource.FullName -Destination $outPath -Force; Write-Host "OK" }

        Remove-Item $tmpZip,$extractDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log verbose "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'ModuleStatus'
                type = 'text'
                label = 'Driver Status'
                text = $Global:ModuleStatus
            }
            @{
                name = 'server'
                type = 'textbox'
                label = 'Server'
                description = 'Hostname/IP of server'
                value = ''
            }
            @{
                name = 'port'
                type = 'textbox'
                label = 'Port'
                description = 'Port of Server'
                value = '3306'
            }
            @{
                name = 'ssl_mode'
                type = 'checkbox'
                label = 'SSL Mode Enabled'
                description = 'Enable SSL Mode'
                value = $false
            }
            @{
                name = 'database'
                type = 'textbox'
                label = 'Database'
                description = 'Name of database'
                value = ''
            }
            @{
                name = 'username'
                type = 'textbox'
                label = 'Username'
                description = 'User account name to access server'
                value = ''
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                description = 'User account password to access server'
                value = ''
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                description = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                description = ''
                value = 30
            }
        )
    }

    if ($TestConnection) {
        $connection = $null
        try {
            $connection = Open-MySqlConnection $ConnectionParams
        }
        finally {
            if ($connection) {
                Close-MySqlConnection
            }
        }
    }

    if ($Configuration) {
        @()
    }

    Log verbose "Done"
}


function Idm-OnUnload {
    Close-MySqlConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}

$SqlInfoCache = @{}


function Fill-SqlInfoCache {
    param (
        [switch] $Force,
        $Connection
    )

    if (-not $Connection) { $Connection = $Global:MySqlConnection }

    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le 600000) {
        return
    }

    $sql_command = New-MySqlCommand -Connection $Connection @"
        SELECT *
        FROM (
            SELECT 
                CONCAT(sc.TABLE_SCHEMA, '.', sc.TABLE_NAME) AS full_object_name,
                'Table' AS object_type,
                sc.COLUMN_NAME,
                (CASE WHEN sc.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END) AS is_primary_key,
                (CASE WHEN sc.EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END) AS is_identity,
                0 AS is_computed,
                (CASE WHEN sc.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END) AS is_nullable
            FROM INFORMATION_SCHEMA.TABLES st
            INNER JOIN INFORMATION_SCHEMA.COLUMNS sc
                ON sc.TABLE_SCHEMA = st.TABLE_SCHEMA
            AND sc.TABLE_NAME   = st.TABLE_NAME
            WHERE st.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
            AND st.TABLE_TYPE = 'BASE TABLE'

            UNION ALL

            SELECT 
                CONCAT(sc.TABLE_SCHEMA, '.', sc.TABLE_NAME) AS full_object_name,
                'View' AS object_type,
                sc.COLUMN_NAME,
                0 AS is_primary_key,
                0 AS is_identity,
                0 AS is_computed,
                (CASE WHEN sc.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END) AS is_nullable
            FROM INFORMATION_SCHEMA.VIEWS v
            INNER JOIN INFORMATION_SCHEMA.COLUMNS sc
                ON sc.TABLE_SCHEMA = v.TABLE_SCHEMA
            AND sc.TABLE_NAME   = v.TABLE_NAME
            WHERE v.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
        ) a
        ORDER BY full_object_name, COLUMN_NAME
"@

    try {
        $result = Invoke-MySqlCommand $sql_command
    }
    finally {
        Dispose-MySqlCommand $sql_command
    }

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    foreach ($row in $result) {
        if ($row.full_object_name -ne $object.full_name) {
            if ($null -ne $object.full_name) {
                $objects.Add($object) | Out-Null
            }

            $object = @{
                full_name = $row.full_object_name
                type      = $row.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }

        $object.columns.Add(@{
            name           = $row.column_name
            is_primary_key = $row.is_primary_key
            is_identity    = $row.is_identity
            is_computed    = $row.is_computed
            is_nullable    = $row.is_nullable
        }) | Out-Null
    }

    if ($null -ne $object.full_name) {
        $objects.Add($object) | Out-Null
    }

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
}

function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log verbose "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"

    if ($Class -eq '') {

        if ($GetMeta) {
            $connection = $null
            try {
                $connection = Open-MySqlConnection $SystemParams

                Fill-SqlInfoCache -Force -Connection $connection

                @(
                    foreach ($object in $Global:SqlInfoCache.Objects) {
                        $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                        if ($object.type -ne 'Table') {
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Read'
                                'Source type' = $object.type
                                'Primary key' = $primary_keys -join ', '
                                'Supported operations' = 'R'
                            }
                        }
                        else {
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Create'
                            }

                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Read'
                                'Source type' = $object.type
                                'Primary key' = $primary_keys -join ', '
                                'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                            }

                            if ($primary_keys) {
                                [ordered]@{
                                    Class = $object.full_name
                                    Operation = 'Update'
                                }

                                [ordered]@{
                                    Class = $object.full_name
                                    Operation = 'Delete'
                                }
                            }
                        }
                    }
                )
            }
            finally {
                if ($connection) {
                    Close-MySqlConnection
                }
            }
        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            $connection = $null
            try {
                $connection = Open-MySqlConnection $SystemParams

                Fill-SqlInfoCache -Connection $connection

                $sql_object = Get-MySqlObjectInfo -Class $Class
                $columns = $sql_object.columns

                switch ($Operation) {
                    'Create' {
                        @{
                            semantics = 'create'
                            parameters = @(
                                $columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (-not $_.is_nullable) { 'mandatory' } else { 'optional' }
                                    }
                                }
                            )
                        }
                        break
                    }

                    'Read' {
                        @(
                            @(
                                $columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        type = 'textbox'
                                        label = "Filter: $($_.name)"
                                        description = 'Optional exact-match filter'
                                        value = ''
                                    }
                                }
                            )
                            @{
                                name = 'selected_columns'
                                type = 'grid'
                                label = 'Include columns'
                                description = 'Selected columns'
                                table = @{
                                    rows = @($columns | ForEach-Object {
                                        @{
                                            name = $_.name
                                            config = @(
                                                if ($_.is_primary_key) { 'Primary key' }
                                                if ($_.is_identity)    { 'Generated' }
                                                if ($_.is_computed)    { 'Computed' }
                                                if ($_.is_nullable)    { 'Nullable' }
                                            ) -join ' | '
                                        }
                                    })
                                    settings_grid = @{
                                        selection = 'multiple'
                                        key_column = 'name'
                                        checkbox = $true
                                        filter = $true
                                        columns = @(
                                            @{
                                                name = 'name'
                                                display_name = 'Name'
                                            }
                                            @{
                                                name = 'config'
                                                display_name = 'Configuration'
                                            }
                                        )
                                    }
                                }
                                value = @($columns | ForEach-Object { $_.name })
                            }
                        )
                        break
                    }

                    'Update' {
                        @{
                            semantics = 'update'
                            parameters = @(
                                $columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                    }
                                }
                                @{
                                    name = '*'
                                    allowance = 'prohibited'
                                }
                            )
                        }
                        break
                    }

                    'Delete' {
                        @{
                            semantics = 'delete'
                            parameters = @(
                                $columns | ForEach-Object {
                                    if ($_.is_primary_key) {
                                        @{
                                            name = $_.name
                                            allowance = 'mandatory'
                                        }
                                    }
                                }
                                @{
                                    name = '*'
                                    allowance = 'prohibited'
                                }
                            )
                        }
                        break
                    }
                }
            }
            finally {
                if ($connection) {
                    Close-MySqlConnection
                }
            }
        }
        else {
            $connection = $null
            try {
                $connection = Open-MySqlConnection $SystemParams

                Fill-SqlInfoCache -Connection $connection

                if (-not $Global:ColumnsInfoCache[$Class]) {
                    $sql_object = Get-MySqlObjectInfo -Class $Class
                    $columns = $sql_object.columns

                    $Global:ColumnsInfoCache[$Class] = @{
                        primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                        identity_col = @($columns | Where-Object { $_.is_identity } | ForEach-Object { $_.name })[0]
                        columns = @($columns | ForEach-Object { $_.name })
                    }
                }

                $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
                $identity_col = $Global:ColumnsInfoCache[$Class].identity_col
                $class_columns = $Global:ColumnsInfoCache[$Class].columns
                $quoted_class = ConvertTo-MySqlIdentifier -Name $Class

                $function_params = ConvertFrom-Json2 $FunctionParams
                $selected_columns = @($function_params['selected_columns'])
                $payload_columns = @($function_params.Keys | Where-Object { $_ -in $class_columns })
                $projection = ConvertTo-MySqlProjection -SelectedColumns $selected_columns -AvailableColumns $class_columns

                $keys_with_null_value = @()
                foreach ($key in $function_params.Keys) {
                    if ($null -eq $function_params[$key]) {
                        $keys_with_null_value += $key
                    }
                }
                foreach ($key in $keys_with_null_value) {
                    $function_params[$key] = [System.DBNull]::Value
                }

                switch ($Operation) {
                    'Create' {
                        if ($payload_columns.Count -eq 0) {
                            throw 'Create requires at least one column value.'
                        }

                        $insert_command = New-MySqlCommand -Connection $connection
                        try {
                            $insert_command.CommandText = @"
                                INSERT INTO $quoted_class (
                                    $(@($payload_columns | ForEach-Object { ConvertTo-MySqlIdentifier -Name $_ }) -join ', ')
                                )
                                VALUES (
                                    $(@($payload_columns | ForEach-Object { AddParam-MySqlCommand $insert_command $function_params[$_] }) -join ', ')
                                )
"@

                            Invoke-MySqlNonQuery -SqlCommand $insert_command

                            $select_command = New-MySqlCommand -Connection $connection
                            try {
                                $where_clause = New-MySqlWhereClause -SqlCommand $select_command -Params $function_params -PrimaryKeys $primary_keys -IdentityColumn $identity_col -FallbackColumns $payload_columns
                                $select_command.CommandText = @"
                                    SELECT
                                        $projection
                                    FROM
                                        $quoted_class
                                    $where_clause
                                    LIMIT 1
"@

                                $rv = Invoke-MySqlCommand $select_command
                                LogIO info 'INSERT' -Out $rv
                                Log info ($rv | ConvertTo-Json)
                                $rv
                            }
                            finally {
                                Dispose-MySqlCommand $select_command
                            }
                        }
                        finally {
                            Dispose-MySqlCommand $insert_command
                        }
                        break
                    }

                    'Read' {
                        $read_command = New-MySqlCommand -Connection $connection
                        try {
                            $where_clause = New-MySqlWhereClause -SqlCommand $read_command -Params $function_params -Columns $payload_columns -AllowEmpty
                            $read_command.CommandText = @"
                                SELECT
                                    $projection
                                FROM
                                    $quoted_class$where_clause
"@

                            Invoke-MySqlCommand $read_command
                        }
                        finally {
                            Dispose-MySqlCommand $read_command
                        }
                        break
                    }

                    'Update' {
                        $update_columns = @($payload_columns | Where-Object { $_ -notin $primary_keys })
                        if ($update_columns.Count -eq 0) {
                            throw 'Update requires at least one non-primary-key column value.'
                        }

                        $update_command = New-MySqlCommand -Connection $connection
                        try {
                            $where_clause = New-MySqlWhereClause -SqlCommand $update_command -Params $function_params -Columns $primary_keys
                            $update_command.CommandText = @"
                                UPDATE
                                    $quoted_class
                                SET
                                    $(@($update_columns | ForEach-Object { "$(ConvertTo-MySqlIdentifier -Name $_) = $(AddParam-MySqlCommand $update_command $function_params[$_])" }) -join ', ')
                                $where_clause
                                LIMIT 1
"@

                            Invoke-MySqlNonQuery -SqlCommand $update_command

                            $select_command = New-MySqlCommand -Connection $connection
                            try {
                                $select_command.CommandText = @"
                                    SELECT
                                        $projection
                                    FROM
                                        $quoted_class
                                    $(New-MySqlWhereClause -SqlCommand $select_command -Params $function_params -Columns $primary_keys)
                                    LIMIT 1
"@

                                $rv = Invoke-MySqlCommand $select_command
                                LogIO info 'UPDATE' -Out $rv
                                Log info ($rv | ConvertTo-Json)
                                $rv
                            }
                            finally {
                                Dispose-MySqlCommand $select_command
                            }
                        }
                        finally {
                            Dispose-MySqlCommand $update_command
                        }
                        break
                    }

                    'Delete' {
                        $delete_command = New-MySqlCommand -Connection $connection
                        try {
                            $delete_command.CommandText = @"
                                DELETE FROM
                                    $quoted_class
                                $(New-MySqlWhereClause -SqlCommand $delete_command -Params $function_params -Columns $primary_keys)
                                LIMIT 1
"@

                            Invoke-MySqlNonQuery -SqlCommand $delete_command
                        }
                        finally {
                            Dispose-MySqlCommand $delete_command
                        }
                        break
                    }
                }
            }
            finally {
                if ($connection) {
                    Close-MySqlConnection
                }
            }
        }

    }

    Log verbose "Done"
}

#
# Helper functions
#

function New-MySqlCommand {
    param (
        [string] $CommandText,
        $Connection
    )

    if (-not $Connection) { $Connection = $Global:MySqlConnection }
    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $CommandText
    $cmd
}


function Dispose-MySqlCommand {
    param (
        $SqlCommand
    )

    if ($SqlCommand) {
        $SqlCommand.Dispose()
    }
}


function ConvertTo-MySqlIdentifier {
    param (
        [string] $Name
    )

    if ([string]::IsNullOrWhiteSpace($Name)) {
        throw 'MySQL identifier cannot be empty.'
    }

    @($Name.Split('.') | ForEach-Object {
        if ([string]::IsNullOrWhiteSpace($_)) {
            throw "Invalid MySQL identifier '$Name'."
        }

        '`' + $_.Replace('`', '``') + '`'
    }) -join '.'
}


function ConvertTo-MySqlProjection {
    param (
        [string[]] $SelectedColumns,
        [string[]] $AvailableColumns
    )

    if (-not $SelectedColumns -or $SelectedColumns.Count -eq 0) {
        return '*'
    }

    $invalid_columns = @($SelectedColumns | Where-Object { $_ -notin $AvailableColumns })
    if ($invalid_columns.Count -gt 0) {
        throw "Unknown selected column(s): $($invalid_columns -join ', ')"
    }

    @($SelectedColumns | ForEach-Object { ConvertTo-MySqlIdentifier -Name $_ }) -join ', '
}


function Get-MySqlObjectInfo {
    param (
        [string] $Class
    )

    $sql_object = $Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class } | Select-Object -First 1
    if (-not $sql_object) {
        throw "Unsupported class '$Class'."
    }

    $sql_object
}


function AddParam-MySqlCommand {
    param (
        $SqlCommand,
        $Param
    )

    $param_name = "@param$($SqlCommand.Parameters.Count)_"
    $p = $SqlCommand.CreateParameter()
    $p.ParameterName = $param_name
    $p.Value = if ($null -eq $Param) { [System.DBNull]::Value } else { $Param }
    $SqlCommand.Parameters.Add($p) | Out-Null

    return $param_name
}


function New-MySqlWhereClause {
    param (
        $SqlCommand,
        $Params,
        [string[]] $Columns,
        [string[]] $PrimaryKeys,
        [string] $IdentityColumn,
        [string[]] $FallbackColumns,
        [switch] $AllowEmpty
    )

    if (-not $Columns -or $Columns.Count -eq 0) {
        if ($IdentityColumn) {
            return " WHERE $(ConvertTo-MySqlIdentifier -Name $IdentityColumn) = LAST_INSERT_ID()"
        }

        if ($PrimaryKeys -and $PrimaryKeys.Count -gt 0) {
            $Columns = $PrimaryKeys
        }
        else {
            $Columns = $FallbackColumns
        }
    }

    $predicates = foreach ($column in $Columns) {
        if ($column -notin $Params.Keys) {
            continue
        }

        $quoted_column = ConvertTo-MySqlIdentifier -Name $column
        if ($Params[$column] -eq [System.DBNull]::Value) {
            "$quoted_column IS NULL"
        }
        else {
            "$quoted_column = $(AddParam-MySqlCommand $SqlCommand $Params[$column])"
        }
    }

    $predicates = @($predicates)
    if ($predicates.Count -eq 0) {
        if ($AllowEmpty) {
            return ''
        }

        throw 'A WHERE clause could not be built from the supplied parameters.'
    }

    " WHERE $($predicates -join ' AND ')"
}


function DeParam-MySqlCommand {
    param (
        $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        $value_txt =
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            elseif ($p.Value -is [DateTime]) {
                "'$($p.Value.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            elseif ($p.Value -is [TimeSpan]) {
                $ts = $p.Value
                $totalHours = [Math]::Floor($ts.TotalHours)
                "'$('{0}:{1:D2}:{2:D2}' -f $totalHours, $ts.Minutes, $ts.Seconds)'"
            }
            elseif ($p.Value -is [string] -and $p.Value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                $parsed = [DateTime]::Parse($p.Value, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
                "'$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            elseif ($p.Value -is [string] -and $p.Value -match '^\d{13}$') {
                $parsed = [DateTimeOffset]::FromUnixTimeMilliseconds([long]$p.Value).UtcDateTime
                "'$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))'"
            }
            else {
                $val = $p.Value.ToString()
                if ($p.Value -is [string]) {
                    "'$($val.Replace("'", "''"))'"
                } else {
                    $val.Replace("'", "''")
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
}


function Invoke-MySqlCommand {
    param (
        $SqlCommand,
        [string] $DeParamCommand
    )

    function Invoke-MySqlCommand-ExecuteReader {
        param (
            $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        try {
            $column_names = for ($i = 0; $i -lt $data_reader.FieldCount; $i++) {
                $data_reader.GetName($i)
            }

            if ($column_names) {
                while ($data_reader.Read()) {
                    $hash_table = [ordered]@{}

                    foreach ($column_name in $column_names) {
                        $value_txt = if ($data_reader[$column_name] -eq [System.DBNull]::Value) {
                                        $null
                                    }
                                    elseif ($data_reader[$column_name] -is [DateTime]) {
                                        "$($data_reader[$column_name].ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    elseif ($data_reader[$column_name] -is [TimeSpan]) {
                                        $ts = $data_reader[$column_name]
                                        $totalHours = [Math]::Floor($ts.TotalHours)
                                        "$('{0}:{1:D2}:{2:D2}' -f $totalHours, $ts.Minutes, $ts.Seconds)"
                                    }
                                    elseif ($data_reader[$column_name] -is [string] -and $data_reader[$column_name] -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                                        $parsed = [DateTime]::Parse($data_reader[$column_name], [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                        "$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    elseif ($data_reader[$column_name] -is [string] -and $data_reader[$column_name] -match '^\d{13}$') {
                                        $parsed = [DateTimeOffset]::FromUnixTimeMilliseconds([long]$data_reader[$column_name]).UtcDateTime
                                        "$($parsed.ToString('yyyy-MM-dd HH:mm:ss'))"
                                    }
                                    else {
                                        if ($data_reader[$column_name] -is [string]) {
                                            "$($data_reader[$column_name])"
                                        } else {
                                            $data_reader[$column_name]
                                        }
                                    }

                        $hash_table[$column_name] = $value_txt
                    }

                    New-Object -TypeName PSObject -Property $hash_table
                }
            }
        }
        finally {
            $data_reader.Dispose()
        }
    }

    if (-not $DeParamCommand) {
        $DeParamCommand = DeParam-MySqlCommand $SqlCommand
    }

    LogIO info ($DeParamCommand -split ' ')[0] -In -Command $DeParamCommand
    Log debug $DeParamCommand

    try {
        Invoke-MySqlCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
        throw
    }

    Log debug "Done"
}


function Invoke-MySqlNonQuery {
    param (
        $SqlCommand,
        [string] $DeParamCommand
    )

    if (-not $DeParamCommand) {
        $DeParamCommand = DeParam-MySqlCommand $SqlCommand
    }

    LogIO info ($DeParamCommand -split ' ')[0] -In -Command $DeParamCommand
    Log debug $DeParamCommand

    try {
        [void]$SqlCommand.ExecuteNonQuery()
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
        throw
    }

    Log debug "Done"
}

function Open-MySqlConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams

    $sb = New-Object MySqlConnector.MySqlConnectionStringBuilder
    $sb.Server = $connection_params.server
    $sb.Port = [int]$connection_params.port
    $sb.Database = $connection_params.database
    $sb.UserID = $connection_params.username
    $sb.Password = $connection_params.password
    $sb.AllowLoadLocalInfile = $true
    $sb.UseAffectedRows = $true
    $sb.AllowUserVariables = $true
    $sb.TlsVersion = 'Tls12'

    if ($connection_params.ssl_mode) {
        $sb.SslMode = [MySqlConnector.MySqlSslMode]::Preferred
    }

    $connection_string = $sb.ToString()

    Log verbose "Opening MySqlConnection server='$($sb.Server)'; port='$($sb.Port)'; database='$($sb.Database)'; user='$($sb.UserID)'; ssl_mode='$($sb.SslMode)'"

    try {
        $connection = New-Object MySqlConnector.MySqlConnection($connection_string)
        $connection.Open()

        $Global:MySqlConnection = $connection
        $Global:MySqlConnectionString = $connection_string

        $Global:ColumnsInfoCache = @{}
        $Global:SqlInfoCache = @{}

        return $connection
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
        throw
    }
}


function Close-MySqlConnection {
    if ($Global:MySqlConnection) {
        Log verbose "Closing MySqlConnection"

        try {
            $Global:MySqlConnection.Close()
            $Global:MySqlConnection.Dispose()
            $Global:MySqlConnection = $null
            $Global:MySqlConnectionString = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log verbose "Done"
    }
}


